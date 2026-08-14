#!/usr/bin/env python3
"""
verify_pipeline.py — the trust layer. Checks that can actually FAIL.

The previous generation of this file had checks that could not fail (every state
was injected with zeros and flagged low_volume, so "all 50 present" was
tautological; the snapshot was recomputed from the same file that produced it, so
the cross-check only tested the aggregation function). This version fixes that.

Checks are labelled STRUCTURAL (properties of the code/schema, pass or fail
regardless of how much data exists) or DATA (require real collected data; before
the first real ingest these WARN "no data yet" rather than PASS, so an empty
pipeline is never mistaken for a verified one).

STRUCTURAL
  S1  no-sentiment guard — no forbidden judgment key anywhere in archive,
      classified, or history items
  S2  synthetic guard — NO item flagged synthetic:true may appear in the shipped
      items_classified.json or media_history.json (fixtures are fine in a temp
      demo dir; they must never reach the real derived files)
  S3  topic tags are all within the locked allowed set
  S6  share math — topic_share == topic_volume / total within rounding

DATA
  D1  archive integrity — archive not shorter than its watermark (truncation
      tripwire); no duplicate ids; every JSONL line parses
  D2  sightings integrity — every sighting id exists in the archive
  D3  de-circularized snapshot — recompute the latest snapshot's per-state TOTALS
      straight from items_archive.jsonl filtered to the snapshot's declared
      window; must match the snapshot (this is what proves the numbers are real)
  D4  dead-feed detection — a state that HAS a registry feed but produced zero
      items across the last N snapshots is a likely dead feed, not a quiet state
  D5  feed-health coverage — report validated vs unvalidated registry feeds

Exit non-zero if any check FAILs (reds the CI run). WARN never fails the build.

Usage:
    python3 verify_pipeline.py
    python3 verify_pipeline.py --data-dir /tmp/demo --dead-feed-window 3
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


def main():
    ap = argparse.ArgumentParser(description="Verify the state-media pipeline.")
    ap.add_argument("--data-dir", help="pipeline data dir (default: script dir)")
    ap.add_argument("--dead-feed-window", type=int, default=3,
                    help="a fed state silent for this many recent snapshots -> FAIL")
    args = ap.parse_args()

    data_dir = store.resolve_data_dir(args.data_dir)
    results = []

    def record(cid, kind, status, msg):
        results.append({"id": cid, "kind": kind, "status": status, "msg": msg})

    allowed = set(json.loads(TOPICS_PATH.read_text())["allowed_topics"])
    registry = json.loads(CONFIG_PATH.read_text()) if CONFIG_PATH.exists() else {"feeds": []}
    fed_states = {f["state"] for f in registry.get("feeds", [])}

    # ── load derived files (may be absent/empty before first ingest) ──
    classified_path = Path(data_dir) / store.CLASSIFIED_NAME
    history_path = Path(data_dir) / store.HISTORY_NAME
    classified = json.loads(classified_path.read_text()) if classified_path.exists() else {"items": []}
    history = json.loads(history_path.read_text()) if history_path.exists() else {"snapshots": []}
    class_items = classified.get("items", [])
    snapshots = history.get("snapshots", [])

    archive = store.load_archive(data_dir)

    # ── STRUCTURAL ────────────────────────────────────────────────────────
    # S1 no-sentiment
    def has_forbidden(objs):
        bad = set()
        for o in objs:
            for k in o.keys():
                if k.lower() in store.FORBIDDEN_KEYS:
                    bad.add(k)
        return bad
    forb = has_forbidden(archive) | has_forbidden(class_items)
    for snap in snapshots:
        forb |= has_forbidden([snap.get("national", {})])
        forb |= has_forbidden(list(snap.get("states", {}).values()))
    if forb:
        record("S1", "STRUCTURAL", "FAIL", f"forbidden judgment key(s) present: {sorted(forb)}")
    else:
        record("S1", "STRUCTURAL", "PASS", "no sentiment/judgment keys anywhere")

    # S2 synthetic guard on shipped derived files
    syn_class = sum(1 for it in class_items if it.get("synthetic"))
    syn_hist = sum(1 for s in snapshots if s.get("meta", {}).get("contains_synthetic"))
    if syn_class or syn_hist:
        record("S2", "STRUCTURAL", "FAIL",
               f"SYNTHETIC data in shipped files: {syn_class} classified item(s), "
               f"{syn_hist} snapshot(s). Fixtures must stay in a temp demo dir.")
    else:
        record("S2", "STRUCTURAL", "PASS", "no synthetic data in classified/history")

    # S3 topics in allowed set
    stray = set()
    for it in class_items:
        stray |= (set(it.get("topics", [])) - allowed)
    if stray:
        record("S3", "STRUCTURAL", "FAIL", f"topic tags outside locked set: {sorted(stray)}")
    else:
        record("S3", "STRUCTURAL", "PASS", f"all topic tags within locked set {sorted(allowed)}")

    # S6 share math
    share_err = []
    for snap in snapshots:
        for st, blk in snap.get("states", {}).items():
            total = blk.get("total", 0)
            for t, share in blk.get("topic_share", {}).items():
                expect = round(blk["topic_volume"].get(t, 0) / total, 4) if total else 0.0
                if abs(expect - share) > 0.0002:
                    share_err.append(f"{snap['date']}/{st}/{t}")
    if share_err:
        record("S6", "STRUCTURAL", "FAIL", f"topic_share != volume/total: {share_err[:6]}")
    else:
        record("S6", "STRUCTURAL", "PASS", "topic_share reproduces volume/total everywhere")

    # ── DATA ──────────────────────────────────────────────────────────────
    # D1 archive integrity
    if not archive and not (Path(data_dir) / store.ARCHIVE_NAME).exists():
        record("D1", "DATA", "WARN", "no archive yet — no real ingest has run")
    else:
        wm = store.read_watermark(data_dir)
        lines = store.count_lines(store.archive_path(data_dir))
        bad_lines = [ln for ln, obj, err in store.iter_jsonl(store.archive_path(data_dir)) if err]
        ids = [obj["id"] for _, obj, err in store.iter_jsonl(store.archive_path(data_dir))
               if obj and obj.get("id")]
        dupes = [k for k, c in Counter(ids).items() if c > 1]
        problems = []
        if lines < int(wm.get("archive_lines") or 0):
            problems.append(f"archive {lines} lines < watermark {wm.get('archive_lines')} (TRUNCATION)")
        if dupes:
            problems.append(f"{len(dupes)} duplicate id(s)")
        if bad_lines:
            problems.append(f"{len(bad_lines)} unparseable line(s)")
        if problems:
            record("D1", "DATA", "FAIL", "; ".join(problems))
        else:
            record("D1", "DATA", "PASS",
                   f"{lines} lines, no dupes, watermark {wm.get('archive_lines')} intact")

    # D2 sightings integrity
    sight_ids = {obj.get("id") for _, obj, err in store.iter_jsonl(store.sightings_path(data_dir)) if obj}
    if not sight_ids:
        record("D2", "DATA", "WARN", "no sightings yet")
    else:
        arch_ids = {it["id"] for it in archive}
        orphans = sight_ids - arch_ids
        if orphans:
            record("D2", "DATA", "FAIL", f"{len(orphans)} sighting(s) reference no archive item")
        else:
            record("D2", "DATA", "PASS", f"{len(sight_ids)} sighting ids all resolve to archive items")

    # D3 de-circularized snapshot cross-check (the important one)
    if not snapshots:
        record("D3", "DATA", "WARN", "no snapshots yet")
    elif not archive:
        record("D3", "DATA", "WARN", "snapshot present but archive empty — cannot cross-check")
    else:
        latest = snapshots[-1]
        ws, we = latest.get("window_start"), latest.get("window_end")
        if not ws or not we:
            record("D3", "DATA", "WARN", "latest snapshot has no window — cannot de-circularize")
        else:
            window_items = store.filter_window(archive, ws, we)
            recompute = Counter(it["state"] for it in window_items if it.get("state") != "US")
            mism = []
            for st, blk in latest.get("states", {}).items():
                if recompute.get(st, 0) != blk.get("total", 0):
                    mism.append(f"{st}: snap={blk.get('total',0)} archive={recompute.get(st,0)}")
            if mism:
                record("D3", "DATA", "FAIL",
                       f"snapshot totals do NOT match archive window [{ws}..{we}]: {mism[:6]}")
            else:
                record("D3", "DATA", "PASS",
                       f"snapshot totals reproduce from archive window [{ws}..{we}] "
                       f"({len(window_items)} items, independent recompute)")

    # D4 dead-feed detection
    if len(snapshots) < args.dead_feed_window:
        record("D4", "DATA", "WARN",
               f"only {len(snapshots)} snapshot(s); need {args.dead_feed_window} to judge dead feeds")
    else:
        recent = snapshots[-args.dead_feed_window:]
        silent_streak = []
        for st in sorted(fed_states):
            totals = [s.get("states", {}).get(st, {}).get("total", 0) for s in recent]
            if all(t == 0 for t in totals):
                silent_streak.append(st)
        if silent_streak:
            record("D4", "DATA", "FAIL",
                   f"{len(silent_streak)} fed state(s) silent for {args.dead_feed_window} "
                   f"snapshots (likely dead feeds): {silent_streak}")
        else:
            record("D4", "DATA", "PASS",
                   f"every fed state produced items within the last {args.dead_feed_window} snapshots")

    # D5 feed-health coverage
    feeds = registry.get("feeds", [])
    validated = sum(1 for f in feeds if f.get("validation", {}).get("validated"))
    if feeds:
        status = "WARN" if validated < len(feeds) else "PASS"
        record("D5", "DATA", status,
               f"{validated}/{len(feeds)} registry feeds live-validated; "
               f"{len(feeds)-validated} still candidate (run discover_feeds.py --health-check)")
    else:
        record("D5", "DATA", "WARN", "empty registry")

    # ── report ────────────────────────────────────────────────────────────
    counts = Counter(r["status"] for r in results)
    report = {
        "_comment": "STRUCTURAL checks hold regardless of data volume; DATA checks "
                    "WARN until a real ingest has run. 'All PASS' with DATA WARNs "
                    "means the code is sound but no real data exists yet.",
        "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "summary": dict(counts),
        "checks": results,
    }
    (Path(data_dir) / store.REPORT_NAME).write_text(json.dumps(report, indent=2))

    for r in results:
        print(f"  [{r['status']}] {r['id']} ({r['kind']}): {r['msg']}")
    print(f"\n{dict(counts)} -> {store.REPORT_NAME}")
    if counts.get("FAIL"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
