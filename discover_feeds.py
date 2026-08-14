#!/usr/bin/env python3
"""
discover_feeds.py — feed discovery + health-check for the source registry.

Human-in-the-loop by design: this script PROPOSES registry changes, it never
auto-edits feeds_config.json. Proposals land in feeds_patch.json; a human
reviews, then apply_feeds_patch.py merges approved entries. This mirrors the
Electoral Dashboard's constants_patch review gate.

Two modes:

  --candidates FILE   Discovery. FILE is JSON: [{"state":"CO","outlet":"...",
                      "feed_url":"...","site_url":"...","discovery_source":"..."}].
                      Each candidate feed is validated (parses cleanly, has
                      recent items) and, if healthy and not already registered,
                      emitted to feeds_patch.json as an "add" proposal.

  --health-check      Re-validate feeds ALREADY in the registry. Feeds that
                      fail to parse or have no items in the last --stale-days
                      are emitted as "flag" proposals (status -> flagged/dead).
                      HEALTHY feeds still marked unvalidated (status "candidate")
                      are emitted as "validate" proposals (promote to "active",
                      set validation.validated=true). Nothing is changed
                      automatically — apply_feeds_patch.py merges after review.
                      This is how the whole registry, which ships as unvalidated
                      candidates, gets confirmed on the first live run.

NETWORK NOTE: like the rest of the pipeline, network fetching uses feedparser
at RUNTIME (GitHub Actions). For sandbox testing, pass --fixtures DIR to
validate against saved XML instead of the live network.

Usage:
    python3 discover_feeds.py --candidates candidates.json
    python3 discover_feeds.py --health-check
    python3 discover_feeds.py --health-check --fixtures fixtures/ --stale-days 30
"""

import argparse
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import feedparser
except ImportError:  # pragma: no cover
    feedparser = None

HERE = Path(__file__).resolve().parent
FEEDS = HERE / "feeds_config.json"
PATCH = HERE / "feeds_patch.json"
USER_AGENT = "Mozilla/5.0 (StateMediaPipeline discovery; +https://parvezpothiawala.com)"


def slugify(s):
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def recent_item_count(parsed, days):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    n = 0
    for e in parsed.entries:
        tp = e.get("published_parsed") or e.get("updated_parsed")
        if tp:
            dt = datetime(*tp[:6], tzinfo=timezone.utc)
            if dt >= cutoff:
                n += 1
        else:
            n += 1  # undated: count it, don't punish feeds with sparse dates
    return n


def validate_feed(source, is_fixture, stale_days):
    """Return (ok, item_count, recent_count, note)."""
    if feedparser is None:
        return False, 0, 0, "feedparser not installed"
    try:
        parsed = feedparser.parse(source) if is_fixture else feedparser.parse(source, agent=USER_AGENT)
    except Exception as e:  # noqa: BLE001
        return False, 0, 0, f"parse error: {e}"
    n = len(parsed.entries)
    if n == 0:
        return False, 0, 0, "no items"
    recent = recent_item_count(parsed, stale_days)
    if getattr(parsed, "bozo", 0) and n == 0:
        return False, n, recent, "malformed and empty"
    return True, n, recent, "ok"


def fixture_for_state(fixtures_dir, state):
    if not fixtures_dir:
        return None
    matches = sorted(Path(fixtures_dir).glob(f"{state}__*.xml"))
    return matches[0] if matches else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--candidates", help="JSON file of candidate feeds to validate")
    ap.add_argument("--health-check", action="store_true",
                    help="re-validate feeds already in the registry")
    ap.add_argument("--fixtures", help="offline: validate against saved XML in DIR")
    ap.add_argument("--stale-days", type=int, default=30,
                    help="a feed with no items newer than this is stale")
    ap.add_argument("--min-recent", type=int, default=1,
                    help="min recent items for a candidate to be proposed")
    args = ap.parse_args()

    if not args.candidates and not args.health_check:
        ap.error("choose --candidates FILE and/or --health-check")

    config = json.loads(FEEDS.read_text())
    registered = {f["feed_url"] for f in config["feeds"]}
    fixtures_dir = args.fixtures

    proposals = []

    if args.candidates:
        cands = json.loads(Path(args.candidates).read_text())
        for c in cands:
            src = fixture_for_state(fixtures_dir, c["state"]) if fixtures_dir else c["feed_url"]
            is_fix = bool(fixtures_dir)
            if src is None:
                proposals.append({"action": "add", "status_proposed": "needs_review",
                                  "feed": c, "reason": "no fixture to validate against (offline)"})
                continue
            ok, n, recent, note = validate_feed(src, is_fix, args.stale_days)
            if c["feed_url"] in registered:
                continue  # already in registry
            if ok and recent >= args.min_recent:
                entry = dict(c)
                entry.setdefault("type", "nonprofit")
                entry["status"] = "active"
                entry["added"] = datetime.now(timezone.utc).date().isoformat()
                entry["validation"] = {"validated": True,
                                       "validated_on": datetime.now(timezone.utc).date().isoformat(),
                                       "method": "discover_feeds validate",
                                       "items": n, "recent_items": recent}
                proposals.append({"action": "add", "feed": entry,
                                  "reason": f"healthy: {n} items, {recent} recent"})
            else:
                proposals.append({"action": "reject", "feed": c,
                                  "reason": f"failed validation: {note} "
                                            f"({n} items, {recent} recent)"})

    if args.health_check:
        for f in config["feeds"]:
            src = fixture_for_state(fixtures_dir, f["state"]) if fixtures_dir else f["feed_url"]
            is_fix = bool(fixtures_dir)
            if src is None:
                # no way to check offline; skip silently
                continue
            ok, n, recent, note = validate_feed(src, is_fix, args.stale_days)
            already_validated = f.get("validation", {}).get("validated")
            today = datetime.now(timezone.utc).date().isoformat()
            if not ok:
                proposals.append({"action": "flag", "feed_url": f["feed_url"],
                                  "outlet": f["outlet"], "state": f["state"],
                                  "status_proposed": "dead", "reason": note})
            elif recent == 0:
                proposals.append({"action": "flag", "feed_url": f["feed_url"],
                                  "outlet": f["outlet"], "state": f["state"],
                                  "status_proposed": "flagged",
                                  "reason": f"stale: 0 items in last {args.stale_days}d ({n} total)"})
            elif not already_validated:
                # healthy but still an unvalidated candidate -> propose promotion
                proposals.append({"action": "validate", "feed_url": f["feed_url"],
                                  "outlet": f["outlet"], "state": f["state"],
                                  "status_proposed": "active",
                                  "validation": {"validated": True, "validated_on": today,
                                                 "method": "discover_feeds --health-check live parse",
                                                 "items_seen": n, "recent_items": recent},
                                  "reason": f"healthy: {n} items, {recent} recent"})

    patch = {
        "_comment": "PROPOSALS ONLY — human review required. Merge approved entries "
                    "with apply_feeds_patch.py. Discovery/health-check never edits "
                    "feeds_config.json directly.",
        "generated": datetime.now(timezone.utc).isoformat(),
        "mode": ("candidates" if args.candidates else "") +
                ("+health-check" if args.health_check else ""),
        "adds": [p for p in proposals if p["action"] == "add"],
        "flags": [p for p in proposals if p["action"] == "flag"],
        "validates": [p for p in proposals if p["action"] == "validate"],
        "rejects": [p for p in proposals if p["action"] == "reject"],
    }
    PATCH.write_text(json.dumps(patch, indent=2))
    print(f"proposals -> {PATCH.name}: "
          f"{len(patch['adds'])} adds, {len(patch['flags'])} flags, "
          f"{len(patch['validates'])} validates, {len(patch['rejects'])} rejects "
          f"(review before applying)")


if __name__ == "__main__":
    main()
