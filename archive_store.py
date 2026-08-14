#!/usr/bin/env python3
"""
archive_store.py — the append-only article archive. The one file that matters.

WHY THIS EXISTS
---------------
RSS feeds are a moving window: most publishers expose 10-50 recent items and
silently drop everything older. If you only keep a transient `items_raw.json`
that each run overwrites, then everything the feed dropped between runs is gone
forever. And a weekly per-state/per-topic rollup is *lossy* by construction — you
cannot get back from "CO had 4 climate items" to which four articles they were.

So the article-level corpus is the only durable asset here, and it is the thing
that makes the interesting work possible later:

  * **Re-classification.** Topic definitions will change (topics_config.json is
    versioned precisely because it will). Re-running a new rubric over history is
    only possible if the articles are still on disk.
  * **Entity-level share-of-voice** (Project 2) needs per-article entity
    mentions, not per-week topic counts.
  * **Cross-state narrative propagation** (Project 3) needs per-article
    timestamps and per-outlet sightings of the *same* story, which is exactly
    what a weekly aggregate destroys.

Hence: `items_archive.jsonl` is **append-only**. One JSON object per line, keyed
by the existing content hash (`id` = sha1(normalized_title | published_date)).
Never rewritten, never truncated, never sorted in place. Writers append and
flush; they do not load-modify-write, so a crash mid-run can at worst leave a
short final line (which the integrity check reports) instead of a destroyed file.

Two companion files, both also append-only:

  * `item_sightings.jsonl` — one line per (item id, feed) sighting. Dedup
    collapses the same wire story carried by N outlets into one archive row;
    the sightings log is where "which outlets/states carried it, and when" is
    preserved. Throwing that away would delete the propagation signal.
  * `ingest_runs.jsonl` — one line per ingest run: counts, dedup rate, mode,
    errors. This is the provenance trail for the verification layer.

Plus one small mutable file, `archive_watermark.json`, holding the high-water
line count. It only ever moves up. `verify_pipeline.py` FAILS if the archive is
shorter than the watermark — that is the tripwire for a truncation or a
rewrite-in-place.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent

ARCHIVE_NAME = "items_archive.jsonl"
SIGHTINGS_NAME = "item_sightings.jsonl"
RUNS_NAME = "ingest_runs.jsonl"
WATERMARK_NAME = "archive_watermark.json"
CLASSIFIED_NAME = "items_classified.json"
HISTORY_NAME = "media_history.json"
REPORT_NAME = "verification_report.json"

# Item keys that classification/rollup are allowed to add. Anything resembling a
# judgment is banned by design (see the no-sentiment rule in the README).
FORBIDDEN_KEYS = {"sentiment", "score", "rating", "favorability", "tone", "bias",
                  "polarity", "stance"}


# ── paths ───────────────────────────────────────────────────────────────────

def resolve_data_dir(explicit: str | None = None) -> Path:
    """Where the pipeline reads/writes its data.

    Order: explicit --data-dir, then $PIPELINE_DATA_DIR, then the script dir.
    Tests and demos point this at a temp dir so a run can never touch the
    shipped files.
    """
    if explicit:
        p = Path(explicit).expanduser().resolve()
    elif os.environ.get("PIPELINE_DATA_DIR"):
        p = Path(os.environ["PIPELINE_DATA_DIR"]).expanduser().resolve()
    else:
        p = HERE
    p.mkdir(parents=True, exist_ok=True)
    return p


def archive_path(data_dir: Path) -> Path:
    return Path(data_dir) / ARCHIVE_NAME


def sightings_path(data_dir: Path) -> Path:
    return Path(data_dir) / SIGHTINGS_NAME


def runs_path(data_dir: Path) -> Path:
    return Path(data_dir) / RUNS_NAME


def watermark_path(data_dir: Path) -> Path:
    return Path(data_dir) / WATERMARK_NAME


# ── append-only primitives ──────────────────────────────────────────────────

def append_jsonl(path: Path, objs) -> int:
    """Append objects as JSON lines. Crash-safe by construction:

    open in 'a' (the OS guarantees the write lands at end-of-file), write, flush,
    fsync. We NEVER read the file, modify a structure and write it back — that
    load-modify-write pattern is how append-only archives get truncated.
    """
    objs = list(objs)
    if not objs:
        return 0
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for o in objs:
            fh.write(json.dumps(o, ensure_ascii=False, sort_keys=True) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    return len(objs)


def iter_jsonl(path: Path):
    """Yield (lineno, obj, error). Never raises on a bad line — the caller
    decides whether a malformed line is fatal (verification) or skippable."""
    path = Path(path)
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as fh:
        for i, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield i, json.loads(line), None
            except Exception as e:  # noqa: BLE001
                yield i, None, str(e)


def load_archive(data_dir: Path, strict: bool = False):
    """Return the archive as a list of item dicts."""
    items, bad = [], []
    for lineno, obj, err in iter_jsonl(archive_path(data_dir)):
        if err:
            bad.append((lineno, err))
            continue
        items.append(obj)
    if bad and strict:
        raise ValueError(f"archive has {len(bad)} unparseable line(s): {bad[:3]}")
    return items


def archive_ids(data_dir: Path) -> set:
    """Ids already in the archive. Read-only scan; this is the dedup gate for a
    new ingest run."""
    out = set()
    for _, obj, err in iter_jsonl(archive_path(data_dir)):
        if obj and obj.get("id"):
            out.add(obj["id"])
    return out


def sighting_keys(data_dir: Path) -> set:
    out = set()
    for _, obj, err in iter_jsonl(sightings_path(data_dir)):
        if obj:
            out.add((obj.get("id"), obj.get("feed_url")))
    return out


def count_lines(path: Path) -> int:
    path = Path(path)
    if not path.exists():
        return 0
    n = 0
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                n += 1
    return n


# ── watermark ───────────────────────────────────────────────────────────────

def read_watermark(data_dir: Path) -> dict:
    p = watermark_path(data_dir)
    if not p.exists():
        return {"archive_lines": 0, "unique_ids": 0, "updated": None}
    try:
        return json.loads(p.read_text())
    except Exception:  # noqa: BLE001
        return {"archive_lines": 0, "unique_ids": 0, "updated": None, "corrupt": True}


def bump_watermark(data_dir: Path, lines: int, unique_ids: int) -> dict:
    """Move the watermark UP only. If the archive is currently shorter than the
    recorded high-water mark, we leave the mark alone so the next verification
    run still trips on the regression."""
    wm = read_watermark(data_dir)
    prev = int(wm.get("archive_lines") or 0)
    new = {
        "_comment": "High-water mark for the append-only archive. Only ever "
                    "increases. verify_pipeline.py FAILS if items_archive.jsonl "
                    "has fewer lines than this.",
        "archive_lines": max(prev, lines),
        "unique_ids": max(int(wm.get("unique_ids") or 0), unique_ids),
        "updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "regression_seen": bool(lines < prev),
    }
    watermark_path(data_dir).write_text(json.dumps(new, indent=2))
    return new


# ── date windows ────────────────────────────────────────────────────────────

def parse_date(s):
    try:
        return date.fromisoformat(str(s)[:10])
    except Exception:  # noqa: BLE001
        return None


def week_window(end, days: int = 7):
    """The rollup window: `days` days ending on (and including) `end`.

    A snapshot records this window explicitly so verification can rebuild the
    exact same item set from the archive without consulting the file the
    snapshot was built from.
    """
    end_d = end if isinstance(end, date) else parse_date(end)
    start_d = end_d - timedelta(days=days - 1)
    return start_d.isoformat(), end_d.isoformat()


def in_window(item, start: str, end: str) -> bool:
    d = parse_date(item.get("published"))
    if d is None:
        return False
    return start <= d.isoformat() <= end


def filter_window(items, start: str, end: str):
    return [it for it in items if in_window(it, start, end)]


def window_dates(items):
    """Sorted set of distinct published dates present in `items`."""
    return sorted({parse_date(it.get("published")).isoformat()
                   for it in items if parse_date(it.get("published"))})


def has_synthetic(obj) -> bool:
    """True if this dict (or any item inside it) is flagged synthetic."""
    if isinstance(obj, dict):
        if obj.get("synthetic") is True:
            return True
        return any(has_synthetic(v) for v in obj.values())
    if isinstance(obj, list):
        return any(has_synthetic(v) for v in obj)
    return False
