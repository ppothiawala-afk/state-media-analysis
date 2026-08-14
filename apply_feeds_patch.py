#!/usr/bin/env python3
"""
apply_feeds_patch.py — merge a human-approved feeds_patch.json into the registry.

The approval gate: discover_feeds.py PROPOSES; a human reviews feeds_patch.json;
this script MERGES. Run with --dry-run first to preview. On apply it writes an
audit copy feeds_patch.applied_<date>.json (mirrors the Electoral Dashboard's
constants_patch.applied_*.json convention).

  adds     -> appended to feeds_config.json (skipped if feed_url already present)
  flags    -> matching feed's status set to the proposed status (flagged/dead)
  validates-> matching feed promoted to "active" + validation block updated
              (this is how the candidate registry becomes validated)
  rejects  -> ignored (informational only)

Usage:
    python3 apply_feeds_patch.py --dry-run
    python3 apply_feeds_patch.py            # apply
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
FEEDS = HERE / "feeds_config.json"
PATCH = HERE / "feeds_patch.json"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="preview, don't write")
    ap.add_argument("--patch", default=str(PATCH))
    args = ap.parse_args()

    config = json.loads(FEEDS.read_text())
    patch = json.loads(Path(args.patch).read_text())
    existing = {f["feed_url"] for f in config["feeds"]}

    added, skipped, flagged = [], [], []

    for p in patch.get("adds", []):
        feed = p["feed"]
        if feed["feed_url"] in existing:
            skipped.append(feed["feed_url"])
            continue
        if not args.dry_run:
            config["feeds"].append(feed)
        existing.add(feed["feed_url"])
        added.append(f"{feed['state']} {feed.get('outlet','?')}")

    flag_status = {p["feed_url"]: p["status_proposed"] for p in patch.get("flags", [])}
    validate_map = {p["feed_url"]: p for p in patch.get("validates", [])}
    validated = []
    for f in config["feeds"]:
        if f["feed_url"] in flag_status:
            new_status = flag_status[f["feed_url"]]
            if not args.dry_run:
                f["status"] = new_status
            flagged.append(f"{f['state']} {f['outlet']} -> {new_status}")
        if f["feed_url"] in validate_map:
            p = validate_map[f["feed_url"]]
            if not args.dry_run:
                f["status"] = p.get("status_proposed", "active")
                f["validation"] = p.get("validation", f.get("validation"))
            validated.append(f"{f['state']} {f['outlet']} -> active/validated")

    config["feed_count"] = len(config["feeds"])
    config["states_covered"] = sorted(set(f["state"] for f in config["feeds"] if f["state"] != "US"))

    print(f"adds: {len(added)} | skipped(existing): {len(skipped)} | "
          f"flags: {len(flagged)} | validates: {len(validated)}")
    for a in added:
        print(f"  + {a}")
    for fl in flagged:
        print(f"  ~ {fl}")
    for v in validated:
        print(f"  ✓ {v}")

    if args.dry_run:
        print("dry-run: no files written.")
        return

    FEEDS.write_text(json.dumps(config, indent=2))
    stamp = datetime.now(timezone.utc).date().isoformat()
    (HERE / f"feeds_patch.applied_{stamp}.json").write_text(json.dumps(patch, indent=2))
    print(f"applied. registry now {config['feed_count']} feeds. "
          f"audit copy: feeds_patch.applied_{stamp}.json")


if __name__ == "__main__":
    main()
