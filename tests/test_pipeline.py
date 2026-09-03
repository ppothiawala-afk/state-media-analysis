#!/usr/bin/env python3
"""
Offline test suite — no network, no API key. Drives the real scripts against the
clearly-labelled synthetic fixtures, into a throwaway temp data dir, so the
shipped data files are never touched.

Run:  python3 -m unittest discover -s tests -v
  or: python3 tests/test_pipeline.py
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "tests" / "fixtures_synthetic"


def run(script, *args, expect_ok=True):
    cmd = [sys.executable, str(ROOT / script), *map(str, args)]
    p = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    if expect_ok and p.returncode != 0:
        raise AssertionError(f"{script} failed ({p.returncode}):\n{p.stdout}\n{p.stderr}")
    return p


def count_lines(path):
    if not Path(path).exists():
        return 0
    return sum(1 for ln in Path(path).read_text().splitlines() if ln.strip())


class PipelineTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="smt_test_")

    def ingest(self):
        return run("fetch_feeds.py", "--fixtures", FIXTURES, "--data-dir", self.tmp)

    # ── ingest + dedup ──────────────────────────────────────────────
    def test_ingest_dedup_and_sightings(self):
        self.ingest()
        arch = Path(self.tmp) / "items_archive.jsonl"
        sights = Path(self.tmp) / "item_sightings.jsonl"
        items = [json.loads(l) for l in arch.read_text().splitlines() if l.strip()]
        # CO and OH both carry the same "clean energy bill" wire story -> 1 archive row
        titles = [i["norm_title"] for i in items]
        clean = [t for t in titles if "clean energy bill" in t]
        self.assertEqual(len(clean), 1, "shared wire story should collapse to one archive row")
        # but both sightings are preserved
        sight_rows = [json.loads(l) for l in sights.read_text().splitlines() if l.strip()]
        clean_id = next(i["id"] for i in items if "clean energy bill" in i["norm_title"])
        clean_sights = [s for s in sight_rows if s["id"] == clean_id]
        self.assertGreaterEqual(len(clean_sights), 2, "both outlets' sightings preserved")

    def test_idempotent_reingest(self):
        self.ingest()
        n1 = count_lines(Path(self.tmp) / "items_archive.jsonl")
        self.ingest()  # same fixtures again
        n2 = count_lines(Path(self.tmp) / "items_archive.jsonl")
        self.assertEqual(n1, n2, "re-ingesting identical content must add zero archive lines")

    def test_synthetic_flag_present(self):
        self.ingest()
        items = [json.loads(l) for l in (Path(self.tmp) / "items_archive.jsonl").read_text().splitlines() if l.strip()]
        self.assertTrue(items and all(i.get("synthetic") is True for i in items),
                        "every fixture-derived item must be flagged synthetic")

    # ── classify ────────────────────────────────────────────────────
    def test_classify_prefilter_drops_offtopic(self):
        self.ingest()
        run("classify.py", "--offline", "--data-dir", self.tmp)
        data = json.loads((Path(self.tmp) / "items_classified.json").read_text())
        by_title = {it["title"]: it for it in data["items"]}
        lib = next((v for k, v in by_title.items() if "library" in k.lower()), None)
        self.assertIsNotNone(lib, "off-topic item is retained but with no topics")
        self.assertEqual(lib["topics"], [], "off-topic item should have zero topics")
        # a clearly climate item is tagged climate
        heat = next((v for k, v in by_title.items() if "heat wave" in k.lower()), None)
        self.assertIn("climate", heat["topics"])

    def test_rebuild_is_deterministic(self):
        self.ingest()
        run("classify.py", "--offline", "--data-dir", self.tmp)
        a = json.loads((Path(self.tmp) / "items_classified.json").read_text())["items"]
        run("classify.py", "--offline", "--rebuild", "--data-dir", self.tmp)
        b = json.loads((Path(self.tmp) / "items_classified.json").read_text())["items"]
        self.assertEqual([(x["id"], x["topics"]) for x in a],
                         [(x["id"], x["topics"]) for x in b],
                         "offline rebuild must reproduce identical tags")

    # ── rollup / share math ─────────────────────────────────────────
    def test_snapshot_share_math(self):
        self.ingest()
        run("classify.py", "--offline", "--data-dir", self.tmp)
        run("append_history.py", "--window-days", "3650", "--data-dir", self.tmp)
        hist = json.loads((Path(self.tmp) / "media_history.json").read_text())
        snap = hist["snapshots"][-1]
        for st, blk in snap["states"].items():
            total = blk["total"]
            for t, share in blk["topic_share"].items():
                expect = round(blk["topic_volume"][t] / total, 4) if total else 0.0
                self.assertAlmostEqual(expect, share, places=4,
                                       msg=f"share math wrong for {st}/{t}")

    # ── verification can FAIL ────────────────────────────────────────
    def test_verify_fails_on_synthetic_in_shipped(self):
        # a full fixtures run PUTS synthetic data into classified+history, which
        # is exactly what S2 must catch.
        self.ingest()
        run("classify.py", "--offline", "--data-dir", self.tmp)
        run("append_history.py", "--window-days", "3650", "--data-dir", self.tmp)
        p = run("verify_pipeline.py", "--data-dir", self.tmp, expect_ok=False)
        self.assertEqual(p.returncode, 1, "verify must fail when synthetic data reaches shipped files")
        report = json.loads((Path(self.tmp) / "verification_report.json").read_text())
        s2 = next(c for c in report["checks"] if c["id"] == "S2")
        self.assertEqual(s2["status"], "FAIL")

    def test_verify_fails_on_duplicate_archive_id(self):
        self.ingest()
        arch = Path(self.tmp) / "items_archive.jsonl"
        first = arch.read_text().splitlines()[0]
        with arch.open("a") as fh:
            fh.write(first + "\n")  # duplicate an existing id
        p = run("verify_pipeline.py", "--data-dir", self.tmp, expect_ok=False)
        report = json.loads((Path(self.tmp) / "verification_report.json").read_text())
        d1 = next(c for c in report["checks"] if c["id"] == "D1")
        self.assertEqual(d1["status"], "FAIL")
        self.assertIn("duplicate", d1["msg"])

    def test_verify_fails_on_truncation(self):
        self.ingest()
        arch = Path(self.tmp) / "items_archive.jsonl"
        lines = arch.read_text().splitlines()
        self.assertGreater(len(lines), 1)
        # watermark recorded the full length; now truncate the archive
        arch.write_text("\n".join(lines[:1]) + "\n")
        p = run("verify_pipeline.py", "--data-dir", self.tmp, expect_ok=False)
        report = json.loads((Path(self.tmp) / "verification_report.json").read_text())
        d1 = next(c for c in report["checks"] if c["id"] == "D1")
        self.assertEqual(d1["status"], "FAIL")
        self.assertIn("TRUNCATION", d1["msg"])

    def test_d4_does_not_flag_national_us_feed(self):
        # Regression: the "US" national feed (Stateline) is registered but its
        # items are counted into NATIONAL totals only, never a per-state cell,
        # so its per-state total is always 0 — that must NOT trip D4's dead-feed
        # check. This false positive failed the weekly rollup for weeks.
        # Build a minimal, self-consistent data dir: 3 archived items (real
        # 2-letter states) in one window, and 3 snapshots whose latest matches
        # the archive window. No "US" state cell appears (as append_history
        # produces), while the shipped registry DOES contain a US feed.
        all_states = ["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID",
                      "IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS",
                      "MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK",
                      "OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV",
                      "WI","WY"]
        # one archived item per real state, all in the latest window
        arch = Path(self.tmp) / "items_archive.jsonl"
        items = [{"id": f"a{i}", "title": s, "link": f"http://x/{i}",
                  "published": "2026-09-03", "state": s, "outlet": s, "norm_title": s}
                 for i, s in enumerate(all_states)]
        arch.write_text("".join(json.dumps(it) + "\n" for it in items))
        (Path(self.tmp) / "archive_watermark.json").write_text(
            json.dumps({"archive_lines": len(items), "unique_ids": len(items), "updated": None}))

        def snap(date, ws, we):
            # every real state present (none silent); NO "US" cell, as append_history produces
            return {"date": date, "window_start": ws, "window_end": we, "window_days": 7,
                    "national": {"topic_volume": {}, "topic_share": {}, "total_items": len(all_states)},
                    "states": {s: {"total": 1, "topic_volume": {}, "topic_share": {},
                                   "outlet_count": 1, "per_outlet": {}, "top_entities": [],
                                   "presence": "covered"} for s in all_states},
                    "meta": {"contains_synthetic": False}}
        history = {"metric": "topic_share_primary", "snapshots": [
            snap("2026-08-20", "2026-08-14", "2026-08-20"),
            snap("2026-08-27", "2026-08-21", "2026-08-27"),
            snap("2026-09-03", "2026-08-28", "2026-09-03"),
        ]}
        (Path(self.tmp) / "media_history.json").write_text(json.dumps(history))
        # empty-but-valid classified file so structural checks have something
        (Path(self.tmp) / "items_classified.json").write_text(json.dumps({"items": []}))

        run("verify_pipeline.py", "--data-dir", self.tmp)  # must exit 0
        report = json.loads((Path(self.tmp) / "verification_report.json").read_text())
        d4 = next(c for c in report["checks"] if c["id"] == "D4")
        self.assertEqual(d4["status"], "PASS", f"D4 wrongly flagged: {d4['msg']}")
        self.assertNotIn("US", d4["msg"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
