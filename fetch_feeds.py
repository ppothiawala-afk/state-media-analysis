#!/usr/bin/env python3
"""
fetch_feeds.py — stage 1: collect RSS/Atom items and APPEND them to the archive.

Walks the feed registry (feeds_config.json), fetches each feed, normalizes items
to a flat schema, dedupes on a content hash, and **appends newly-seen items to
`items_archive.jsonl`** (see archive_store.py for why the archive is the centre
of gravity of this pipeline). Items whose id is already in the archive are
skipped, so re-running ingest is idempotent: the same feed content twice adds
zero new lines.

This stage is CHEAP and API-FREE, so it is designed to run **daily** — RSS
windows expire and anything the feed drops between runs is lost forever.
Classification and rollup (`run_rollup.sh`) run weekly.

Nothing here writes a transient whole-corpus file any more. The old
`items_raw.json` (overwritten every run, so lossy) is gone; downstream stages
read the archive.

NO sentiment, NO classification here — this stage only records countable facts
(title, link, published, outlet, state, feed_url). Classification is classify.py.

SYNTHETIC DATA GUARD: any item collected in `--fixtures` mode, or from a feed
whose <generator> is `synthetic-test-fixture`, is stamped `"synthetic": true`.
That flag rides along through every downstream stage and `verify_pipeline.py`
FAILS if it ever reaches items_classified.json or media_history.json.

Usage:
    python3 fetch_feeds.py                                   # live network fetch
    python3 fetch_feeds.py --states CO,OH,HI                 # limit to states
    python3 fetch_feeds.py --fixtures tests/fixtures_synthetic/   # offline, SYNTHETIC
    python3 fetch_feeds.py --max-items 40                    # cap items per feed
    python3 fetch_feeds.py --data-dir /tmp/demo              # write elsewhere
"""

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import archive_store as store

HERE = Path(__file__).resolve().parent
CONFIG_PATH = HERE / "feeds_config.json"
USER_AGENT = "Mozilla/5.0 (StateMediaPipeline collector; +https://parvezpothiawala.com)"
SYNTHETIC_GENERATOR = "synthetic-test-fixture"

# fixture filenames look like [synthetic_]<STATE>__<slug>.xml
FIXTURE_RE = re.compile(r"^(?:synthetic_)?([A-Z]{2})__(.+)$")


def slugify(outlet: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", outlet.lower()).strip("-")


def normalize_title(title: str) -> str:
    """Lowercase, strip an outlet-name suffix after a bullet/pipe, collapse
    whitespace and punctuation. This is the dedup key basis — the SAME wire
    story reprinted by many outlets normalizes identically."""
    t = title or ""
    t = re.split(r"\s+[•|]\s+", t)[0]
    t = t.lower()
    t = re.sub(r"[^a-z0-9 ]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def dedup_hash(norm_title: str, date: str) -> str:
    return hashlib.sha1(f"{norm_title}|{date}".encode("utf-8")).hexdigest()[:16]


# ── parsing (feedparser, with a stdlib fallback so offline tests never need it) ──

def _iso_from_struct(tp) -> str:
    return datetime(*tp[:6], tzinfo=timezone.utc).date().isoformat()


def _parse_with_feedparser(source, is_fixture: bool) -> dict:
    import feedparser
    parsed = (feedparser.parse(str(source)) if is_fixture
              else feedparser.parse(source, agent=USER_AGENT))
    entries = []
    for e in parsed.entries:
        published = ""
        for key in ("published_parsed", "updated_parsed"):
            if e.get(key):
                published = _iso_from_struct(e.get(key))
                break
        if not published:
            for key in ("published", "updated", "date"):
                if e.get(key):
                    published = str(e.get(key))[:10]
                    break
        entries.append({
            "title": (e.get("title") or "").strip(),
            "link": (e.get("link") or "").strip(),
            "summary": e.get("summary", "") or "",
            "published": published,
        })
    return {"generator": (parsed.feed.get("generator") or "") if parsed.get("feed") else "",
            "entries": entries,
            "bozo": bool(getattr(parsed, "bozo", 0))}


def _stdlib_date(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(raw).astimezone(timezone.utc).date().isoformat()
    except Exception:  # noqa: BLE001
        pass
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date().isoformat()
    except Exception:  # noqa: BLE001
        return raw[:10]


def _parse_with_stdlib(path) -> dict:
    """Minimal RSS/Atom reader for fixture files. Exists so the offline test
    suite has zero third-party dependencies; the live path prefers feedparser."""
    import xml.etree.ElementTree as ET
    root = ET.parse(str(path)).getroot()
    ns = {"a": "http://www.w3.org/2005/Atom"}

    def text(el, *names):
        for n in names:
            found = el.find(n) if not n.startswith("a:") else el.find(n, ns)
            if found is not None and (found.text or "").strip():
                return found.text.strip()
            if found is not None and n in ("link", "a:link"):
                href = found.get("href")
                if href:
                    return href.strip()
        return ""

    channel = root.find("channel")
    if channel is not None:
        gen = text(channel, "generator")
        raw_items = channel.findall("item")
        entries = [{
            "title": text(it, "title"),
            "link": text(it, "link"),
            "summary": text(it, "description"),
            "published": _stdlib_date(text(it, "pubDate", "date")),
        } for it in raw_items]
    else:  # Atom
        gen = text(root, "a:generator")
        entries = [{
            "title": text(it, "a:title"),
            "link": text(it, "a:link"),
            "summary": text(it, "a:summary"),
            "published": _stdlib_date(text(it, "a:updated", "a:published")),
        } for it in root.findall("a:entry", ns)]
    return {"generator": gen, "entries": entries, "bozo": False}


def parse_feed_source(source, is_fixture: bool) -> dict:
    try:
        return _parse_with_feedparser(source, is_fixture)
    except ImportError:
        if not is_fixture:
            raise RuntimeError("feedparser is required for live fetches: "
                               "pip install -r requirements.txt")
        return _parse_with_stdlib(source)


# ── collection ──────────────────────────────────────────────────────────────

def collect_from_entries(parsed, feed_meta, max_items, synthetic: bool):
    rows = []
    for entry in parsed["entries"][:max_items]:
        title = entry["title"]
        link = entry["link"]
        if not title or not link:
            continue
        summary = re.sub(r"<[^>]+>", " ", entry.get("summary") or "")
        summary = re.sub(r"\s+", " ", summary).strip()
        norm = normalize_title(title)
        date = entry["published"]
        row = {
            "id": dedup_hash(norm, date),
            "title": title,
            "norm_title": norm,
            "summary": summary[:600],
            "link": link,
            "published": date,
            "state": feed_meta["state"],
            "outlet": feed_meta["outlet"],
            "feed_url": feed_meta["feed_url"],
        }
        if synthetic:
            # Never silently drop the marker: fixture-derived items must be
            # identifiable as fiction everywhere downstream.
            row["synthetic"] = True
        rows.append(row)
    return rows


def load_fixture_map(fixtures_dir: Path):
    """Map state -> [fixture paths]. Filenames must match
    [synthetic_]<STATE>__<slug>.xml; anything else is ignored."""
    out = {}
    for p in sorted(Path(fixtures_dir).glob("*.xml")):
        m = FIXTURE_RE.match(p.stem)
        if not m:
            continue
        out.setdefault(m.group(1).upper(), []).append(p)
    return out


def main():
    ap = argparse.ArgumentParser(description="Collect state-media RSS items into the append-only archive.")
    ap.add_argument("--states", help="comma-separated 2-letter states to limit to")
    ap.add_argument("--fixtures", help="offline: directory of SYNTHETIC feed XML files")
    ap.add_argument("--max-items", type=int, default=60, help="cap items per feed")
    ap.add_argument("--data-dir", help="where to read/write pipeline data (default: script dir)")
    ap.add_argument("--dry-run", action="store_true", help="report what would be appended; write nothing")
    args = ap.parse_args()

    data_dir = store.resolve_data_dir(args.data_dir)
    config = json.loads(CONFIG_PATH.read_text())
    feeds = config["feeds"]
    only = {s.strip().upper() for s in args.states.split(",")} if args.states else None
    if only:
        feeds = [f for f in feeds if f["state"] in only]

    fixtures_dir = Path(args.fixtures) if args.fixtures else None
    fixture_map = load_fixture_map(fixtures_dir) if fixtures_dir else {}
    offline = fixtures_dir is not None
    if offline:
        print(f">> FIXTURE MODE ({fixtures_dir}) — every item collected will be "
              f"stamped synthetic:true and is NOT real data.")

    all_rows = []
    per_feed_counts = []
    errors = []
    feeds_ok = feeds_err = 0

    for f in feeds:
        state = f["state"]
        if offline:
            paths = fixture_map.get(state, [])
            if not paths:
                continue
            sources = [(p, True) for p in paths]
        else:
            sources = [(f["feed_url"], False)]

        for src, is_fix in sources:
            try:
                parsed = parse_feed_source(src, is_fix)
                synthetic = is_fix or SYNTHETIC_GENERATOR in (parsed.get("generator") or "")
                rows = collect_from_entries(parsed, f, args.max_items, synthetic)
                all_rows.extend(rows)
                per_feed_counts.append({"state": state, "outlet": f["outlet"],
                                        "source": str(src), "items": len(rows),
                                        "synthetic": synthetic})
                feeds_ok += 1
                print(f"  + {state} {f['outlet']}: {len(rows)} items"
                      + ("  [SYNTHETIC]" if synthetic else ""))
                if not offline:
                    time.sleep(0.5)  # be polite to servers
            except Exception as e:  # noqa: BLE001
                feeds_err += 1
                errors.append({"state": state, "outlet": f["outlet"], "error": str(e)})
                print(f"  ! {state} {f['outlet']} failed: {e}", file=sys.stderr)

    # ── dedup within this run: same wire story from N outlets -> one archive row,
    #    N sighting rows (the sightings log is what preserves propagation).
    run_seen = {}
    run_items = []
    sightings = []
    collapsed = 0
    for r in all_rows:
        sightings.append({"id": r["id"], "outlet": r["outlet"], "state": r["state"],
                          "feed_url": r["feed_url"], "published": r["published"],
                          "synthetic": bool(r.get("synthetic"))})
        if r["id"] in run_seen:
            collapsed += 1
            continue
        run_seen[r["id"]] = r
        run_items.append(r)

    # ── dedup against what the archive already holds (idempotent re-ingest)
    known_ids = store.archive_ids(data_dir)
    known_sightings = store.sighting_keys(data_dir)
    now = datetime.now(timezone.utc)
    run_id = now.isoformat(timespec="seconds")

    new_items = []
    for r in run_items:
        if r["id"] in known_ids:
            continue
        row = dict(r)
        row["first_seen"] = now.date().isoformat()
        row["ingest_run"] = run_id
        row["source_mode"] = "fixtures" if offline else "live"
        new_items.append(row)

    new_sightings = []
    for s in sightings:
        key = (s["id"], s["feed_url"])
        if key in known_sightings:
            continue
        known_sightings.add(key)
        s = dict(s)
        s["seen"] = now.date().isoformat()
        s["ingest_run"] = run_id
        new_sightings.append(s)

    total_in = len(all_rows)
    dedup_rate = round(collapsed / total_in, 4) if total_in else 0.0

    if args.dry_run:
        print(f"\n[dry-run] would append {len(new_items)} items and "
              f"{len(new_sightings)} sightings to {data_dir}")
        return

    appended = store.append_jsonl(store.archive_path(data_dir), new_items)
    store.append_jsonl(store.sightings_path(data_dir), new_sightings)

    lines = store.count_lines(store.archive_path(data_dir))
    wm = store.bump_watermark(data_dir, lines, lines)

    store.append_jsonl(store.runs_path(data_dir), [{
        "run": run_id,
        "mode": "fixtures" if offline else "live",
        "synthetic_run": offline,
        "feeds_attempted": feeds_ok + feeds_err,
        "feeds_ok": feeds_ok,
        "feeds_err": feeds_err,
        "items_collected": total_in,
        "duplicates_collapsed_in_run": collapsed,
        "dedup_rate": dedup_rate,
        "items_new_to_archive": appended,
        "items_already_archived": len(run_items) - appended,
        "sightings_new": len(new_sightings),
        "archive_lines_after": lines,
        "errors": errors[:20],
        "per_feed": per_feed_counts,
    }])

    print(f"\ncollected {total_in} ({collapsed} collapsed in-run, dedup_rate={dedup_rate})")
    print(f"appended {appended} NEW items ({len(run_items) - appended} already archived) "
          f"-> {store.ARCHIVE_NAME} now {lines} lines "
          f"(watermark {wm['archive_lines']})")
    if wm.get("regression_seen"):
        print("  ! archive is SHORTER than the recorded watermark — run "
              "verify_pipeline.py, the archive may have been truncated.",
              file=sys.stderr)


if __name__ == "__main__":
    main()
