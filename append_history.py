#!/usr/bin/env python3
"""
append_history.py — stage 3: build the weekly coverage time-series (the lookback
product).

Reads items_classified.json, aggregates the trailing window, and appends ONE
snapshot per run-date to media_history.json. History is the actual product here:
a single snapshot is nearly worthless; months of them are the asset.

PRIMARY METRIC IS SHARE, NOT RAW VOLUME.
Raw counts conflate "this outlet publishes a lot" with "this topic is hot in
this state." So each state's snapshot carries:
  * topic_share  — topic_volume / state_total (the primary, comparable measure)
  * topic_volume — absolute counts (secondary, for drill-down and sanity)
  * total, outlet_count, per_outlet — so coverage volume is never mistaken for
    breadth of sourcing (one prolific outlet != many outlets agreeing)
National block carries the same share/volume pair.

The snapshot RECORDS ITS WINDOW ("window_start".."window_end"). verify_pipeline
uses that window to recompute the item set straight from items_archive.jsonl —
independent of this file — so the V4 cross-check is not circular.

State presence is honest: a state with zero items is recorded, but classified as
one of {no_feed, silent, low_volume} rather than a blanket "low_volume", so a
dead feed is distinguishable from a genuinely quiet week (verify uses this).

Idempotent: re-running for a date REPLACES that date's snapshot.
Counts only — no sentiment, no judgment.

Usage:
    python3 append_history.py
    python3 append_history.py --window-days 7 --low-volume-threshold 3
    python3 append_history.py --data-dir /tmp/demo
"""

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import archive_store as store

HERE = Path(__file__).resolve().parent
TOPICS_PATH = HERE / "topics_config.json"
CONFIG_PATH = HERE / "feeds_config.json"

ALL_STATES = ["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL",
              "IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT",
              "NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI",
              "SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"]


def shares(volume: dict, total: int):
    if total <= 0:
        return {t: 0.0 for t in volume}
    return {t: round(volume[t] / total, 4) for t in volume}


def main():
    ap = argparse.ArgumentParser(description="Append a weekly coverage snapshot (share-primary).")
    ap.add_argument("--window-days", type=int, default=7,
                    help="snapshot covers this many days ending on the run date")
    ap.add_argument("--low-volume-threshold", type=int, default=3,
                    help="states with 1..N-1 items are 'low_volume'")
    ap.add_argument("--data-dir", help="pipeline data dir (default: script dir)")
    args = ap.parse_args()

    data_dir = store.resolve_data_dir(args.data_dir)
    classified_path = Path(data_dir) / store.CLASSIFIED_NAME
    if not classified_path.exists():
        raise SystemExit("items_classified.json not found — run classify.py first.")
    data = json.loads(classified_path.read_text())

    allowed = json.loads(TOPICS_PATH.read_text())["allowed_topics"]
    # which states even have a feed in the registry (to tell no_feed from silent)
    states_with_feed = set()
    if CONFIG_PATH.exists():
        for f in json.loads(CONFIG_PATH.read_text()).get("feeds", []):
            states_with_feed.add(f["state"])

    run_date = data.get("generated") or datetime.now(timezone.utc).date().isoformat()
    win_start, win_end = store.week_window(run_date, days=args.window_days)

    # window-limit the classified items so the snapshot matches its declared window
    items = [it for it in data["items"]
             if win_start <= str(it.get("published", ""))[:10] <= win_end]

    per_state_topics = defaultdict(Counter)
    per_state_total = Counter()
    per_state_entities = defaultdict(Counter)
    per_state_outlets = defaultdict(Counter)
    national_topics = Counter()
    national_total = 0

    for it in items:
        st = it["state"]
        topics = it.get("topics", [])
        if st == "US":  # national wire — national totals only
            for t in topics:
                national_topics[t] += 1
            national_total += 1
            continue
        per_state_total[st] += 1
        national_total += 1
        per_state_outlets[st][it.get("outlet", "?")] += 1
        for t in topics:
            per_state_topics[st][t] += 1
            national_topics[t] += 1
        for e in it.get("entities", []):
            per_state_entities[st][e] += 1

    states_block = {}
    presence_counts = Counter()
    for st in ALL_STATES:
        total = per_state_total.get(st, 0)
        vol = {t: per_state_topics[st].get(t, 0) for t in allowed}
        if total == 0:
            status = "no_feed" if st not in states_with_feed else "silent"
            presence_counts[status] += 1
            states_block[st] = {
                "total": 0, "topic_volume": vol, "topic_share": shares(vol, 0),
                "outlet_count": 0, "per_outlet": {}, "top_entities": [],
                "presence": status,
            }
            continue
        status = "low_volume" if total < args.low_volume_threshold else "covered"
        presence_counts[status] += 1
        states_block[st] = {
            "total": total,
            "topic_volume": vol,
            "topic_share": shares(vol, total),
            "outlet_count": len(per_state_outlets[st]),
            "per_outlet": dict(per_state_outlets[st]),
            "top_entities": per_state_entities[st].most_common(8),
            "presence": status,
        }

    nat_vol = {t: national_topics.get(t, 0) for t in allowed}
    snapshot = {
        "date": run_date,
        "window_start": win_start,
        "window_end": win_end,
        "window_days": args.window_days,
        "national": {
            "topic_volume": nat_vol,
            "topic_share": shares(nat_vol, national_total),
            "total_items": national_total,
        },
        "states": states_block,
        "meta": {
            "presence_breakdown": dict(presence_counts),
            "states_covered": presence_counts.get("covered", 0),
            "states_low_volume": presence_counts.get("low_volume", 0),
            "states_silent": presence_counts.get("silent", 0),
            "states_no_feed": presence_counts.get("no_feed", 0),
            "backend": data.get("backend"),
            "topics_version": data.get("topics_version"),
            "contains_synthetic": any(it.get("synthetic") for it in items),
        },
    }

    history_path = Path(data_dir) / store.HISTORY_NAME
    if history_path.exists():
        hist = json.loads(history_path.read_text())
    else:
        hist = {"_comment": "Weekly coverage snapshots. PRIMARY metric is "
                            "topic_share (topic_volume / state_total); absolute "
                            "volume secondary. Counts only, no sentiment. "
                            "History is the product.",
                "metric": "topic_share_primary",
                "snapshots": []}

    before = len(hist["snapshots"])
    hist["snapshots"] = [s for s in hist["snapshots"] if s["date"] != run_date]
    replaced = before != len(hist["snapshots"])
    hist["snapshots"].append(snapshot)
    hist["snapshots"].sort(key=lambda s: s["date"])
    history_path.write_text(json.dumps(hist, indent=1))

    verb = "replaced" if replaced else "appended"
    m = snapshot["meta"]
    print(f"{verb} snapshot {run_date} [{win_start}..{win_end}]: "
          f"{m['states_covered']} covered, {m['states_low_volume']} low, "
          f"{m['states_silent']} silent, {m['states_no_feed']} no-feed · "
          f"{national_total} items · {len(hist['snapshots'])} snapshots total")
    if m["contains_synthetic"]:
        print("  ! snapshot contains SYNTHETIC items — verify_pipeline will FAIL "
              "if this ships as real history.")


if __name__ == "__main__":
    main()
