#!/usr/bin/env python3
"""
classify.py — stage 2: topic-tag + entity-extract the archived articles.

Reads the append-only archive (items_archive.jsonl) — NOT a transient file —
runs a cheap deterministic keyword PRE-FILTER, then tags survivors with the four
locked topics and extracts entity mentions. Writes items_classified.json, a
derived, fully-rebuildable view of the archive.

Because the archive is the source of truth, classification is disposable: if the
topic rubric changes (topics_config.json is versioned for exactly this reason),
`--rebuild` re-classifies the entire archive from scratch. Normal runs are
incremental — items already classified keep their tags, only new archive ids are
processed, which is what bounds API cost.

Two backends:
  * default  — batched Anthropic API calls (claude-haiku-4-5-20251001).
               ONLY keyword-pre-filter survivors are ever sent, so volume is
               bounded; requires ANTHROPIC_API_KEY.
  * --offline — no API. Topics come from which keyword lists matched; entities
               from a regex + gazetteer heuristic. Deterministic; used by the
               test suite and any key-free run.

HARD RULE: outputs are countable facts only — topic tags (from the locked
allowed set), entity mentions, state, outlet, feed, link, date. NO sentiment,
tone, stance, or any judgment score. The forbidden-key guard in archive_store
and verify_pipeline enforce this structurally.

The `synthetic` flag on any archive item rides through to the classified item so
fixture-derived data can never masquerade as real downstream.

Usage:
    python3 classify.py --offline
    python3 classify.py                       # API mode (needs ANTHROPIC_API_KEY)
    python3 classify.py --rebuild --offline   # re-tag the whole archive
    python3 classify.py --data-dir /tmp/demo --offline
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import archive_store as store

HERE = Path(__file__).resolve().parent
TOPICS_PATH = HERE / "topics_config.json"


def load_topics():
    return json.loads(TOPICS_PATH.read_text())


def prefilter_match(text: str, keyword_lists: dict):
    low = text.lower()
    matched = set()
    for topic, words in keyword_lists.items():
        for w in words:
            if w.lower() in low:
                matched.add(topic)
                break
    return matched


# ── offline entity extraction (heuristic, no judgment) ──────────────────────

STOPWORD_CAPS = {"The", "A", "An", "This", "That", "It", "He", "She", "They",
                 "In", "On", "At", "For", "And", "But", "Or", "Of", "To",
                 "Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
                 "Saturday", "Sunday", "January", "February", "March", "April",
                 "May", "June", "July", "August", "September", "October",
                 "November", "December"}


def extract_entities_offline(text: str, gazetteer):
    """Capitalized multi-word proper nouns + known gazetteer terms. A mention
    list only — no scoring, no ranking by importance."""
    ents = set()
    for m in re.finditer(r"\b([A-Z][a-zA-Z.]+(?:\s+[A-Z][a-zA-Z.]+){0,3})\b", text):
        phrase = m.group(1).strip(".")
        first = phrase.split()[0]
        if first in STOPWORD_CAPS and " " not in phrase:
            continue
        if len(phrase) < 3:
            continue
        ents.add(phrase)
    for g in gazetteer:
        if g.lower() in text.lower():
            ents.add(g)
    return sorted(ents)[:12]


# ── API tagging ─────────────────────────────────────────────────────────────

def classify_batch_api(client, model, batch, allowed_topics):
    lines = []
    for i, it in enumerate(batch):
        lines.append(f"[{i}] TITLE: {it['title']}\n    SUMMARY: {it.get('summary','')[:300]}")
    joined = "\n".join(lines)
    prompt = (
        "You are a classification function. For each numbered news item, return "
        "ONLY objective, countable facts. Do NOT assess sentiment, tone, bias, "
        "stance, or importance.\n\n"
        f"Allowed topic tags (choose all that apply, may be empty): {allowed_topics}\n"
        "Definitions are locked; tag on the substance of the item.\n"
        "Also list proper-noun ENTITIES mentioned (people, organizations, "
        "agencies, companies, industries, places) — mentions only, no ranking.\n\n"
        "Return STRICT JSON: {\"results\":[{\"i\":0,\"topics\":[...],\"entities\":[...]}, ...]}\n\n"
        f"ITEMS:\n{joined}"
    )
    msg = client.messages.create(
        model=model, max_tokens=2000,
        messages=[{"role": "user", "content": prompt}],
    )
    text = msg.content[0].text
    text = re.sub(r"^```(json)?|```$", "", text.strip(), flags=re.MULTILINE).strip()
    data = json.loads(text)
    out = {}
    for r in data.get("results", []):
        idx = r.get("i")
        topics = [t for t in r.get("topics", []) if t in allowed_topics]
        out[idx] = {"topics": topics, "entities": r.get("entities", [])[:12]}
    return out


def build_entry(it, topics, entities):
    entry = {
        "id": it["id"],
        "title": it["title"],
        "link": it["link"],
        "published": it["published"],
        "state": it["state"],
        "outlet": it["outlet"],
        "feed_url": it.get("feed_url", ""),
        "topics": sorted(set(topics)),
        "entities": entities,
    }
    if it.get("synthetic"):
        entry["synthetic"] = True
    return entry


def main():
    ap = argparse.ArgumentParser(description="Topic-tag + entity-extract archived items.")
    ap.add_argument("--offline", action="store_true",
                    help="no API: tag from keyword matches, heuristic entities")
    ap.add_argument("--rebuild", action="store_true",
                    help="re-classify the ENTIRE archive (ignore cached tags)")
    ap.add_argument("--batch-size", type=int, default=20)
    ap.add_argument("--max-api-items", type=int, default=1000,
                    help="hard cap on items sent to the API per run (cost control)")
    ap.add_argument("--model", default="claude-haiku-4-5-20251001")
    ap.add_argument("--data-dir", help="pipeline data dir (default: script dir)")
    args = ap.parse_args()

    data_dir = store.resolve_data_dir(args.data_dir)
    topics_cfg = load_topics()
    allowed = topics_cfg["allowed_topics"]
    keyword_lists = {k: v for k, v in topics_cfg["keyword_prefilter"].items()
                     if not k.startswith("_")}
    gazetteer = topics_cfg["entity_extraction"]["gazetteer_hint"]

    archive = store.load_archive(data_dir)
    if not archive:
        # Honest empty state — write a valid empty classified file so the
        # dashboard renders "no data yet" instead of erroring.
        out = {
            "_comment": "Derived from items_archive.jsonl. Counts, not judgments: "
                        "no sentiment anywhere. Rebuildable via classify.py --rebuild.",
            "generated": datetime.now(timezone.utc).date().isoformat(),
            "backend": "none",
            "topics_version": topics_cfg.get("version"),
            "allowed_topics": allowed,
            "stats": {"archive_items": 0, "classified": 0},
            "items": [],
        }
        (Path(data_dir) / store.CLASSIFIED_NAME).write_text(json.dumps(out, indent=2))
        print("archive is empty — wrote empty items_classified.json (honest no-data state)")
        return

    # cache of prior classifications (incremental cost control)
    cached = {}
    prev_path = Path(data_dir) / store.CLASSIFIED_NAME
    if prev_path.exists() and not args.rebuild:
        try:
            for it in json.loads(prev_path.read_text()).get("items", []):
                cached[it["id"]] = it
        except Exception:  # noqa: BLE001
            cached = {}

    # Stage A: keyword pre-filter (the cost gate) over items needing work
    survivors, passthru_cached, skipped = [], [], 0
    for it in archive:
        if it["id"] in cached:
            passthru_cached.append(cached[it["id"]])
            continue
        text = f"{it.get('title','')} {it.get('summary','')}"
        matched = prefilter_match(text, keyword_lists)
        it["_prefilter_topics"] = sorted(matched)
        if matched:
            survivors.append(it)
        else:
            skipped += 1  # off-topic: recorded as classified-with-no-topics below
            it["_prefilter_topics"] = []
            passthru_cached.append(build_entry(it, [], []))

    classified = list(passthru_cached)
    api_calls = api_items = 0

    if args.offline:
        for it in survivors:
            text = f"{it.get('title','')} {it.get('summary','')}"
            classified.append(build_entry(it, it["_prefilter_topics"],
                                           extract_entities_offline(text, gazetteer)))
        backend = "offline_keyword"
    else:
        try:
            import anthropic
        except ImportError:
            print("anthropic package required for API mode: pip install -r requirements.txt",
                  file=sys.stderr)
            sys.exit(2)
        key = os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            print("ANTHROPIC_API_KEY not set. Use --offline for a key-free run.",
                  file=sys.stderr)
            sys.exit(2)
        client = anthropic.Anthropic(api_key=key)
        to_send = survivors[:args.max_api_items]
        overflow = survivors[args.max_api_items:]
        for start in range(0, len(to_send), args.batch_size):
            batch = to_send[start:start + args.batch_size]
            try:
                results = classify_batch_api(client, args.model, batch, allowed)
            except Exception as e:  # noqa: BLE001
                print(f"  ! API batch failed ({e}); keyword-tagging this batch",
                      file=sys.stderr)
                results = {}
            api_calls += 1
            api_items += len(batch)
            for i, it in enumerate(batch):
                r = results.get(i)
                if r is None:
                    topics = it["_prefilter_topics"]
                    ents = extract_entities_offline(f"{it.get('title','')} {it.get('summary','')}", gazetteer)
                else:
                    topics = r["topics"] or it["_prefilter_topics"]
                    ents = r["entities"]
                classified.append(build_entry(it, topics, ents))
        for it in overflow:  # beyond cap: keyword-tag, never silently drop
            classified.append(build_entry(
                it, it["_prefilter_topics"],
                extract_entities_offline(f"{it.get('title','')} {it.get('summary','')}", gazetteer)))
        backend = f"anthropic:{args.model}"

    # stable order by (published, id) for deterministic diffs
    classified.sort(key=lambda x: (x.get("published", ""), x["id"]))
    synthetic_n = sum(1 for x in classified if x.get("synthetic"))

    out = {
        "_comment": "Derived from items_archive.jsonl. Counts, not judgments: no "
                    "sentiment anywhere; topics from the locked allowed set only. "
                    "Rebuildable via classify.py --rebuild.",
        "generated": datetime.now(timezone.utc).date().isoformat(),
        "backend": backend,
        "topics_version": topics_cfg.get("version"),
        "allowed_topics": allowed,
        "stats": {
            "archive_items": len(archive),
            "reused_cached": len(cached),
            "newly_prefiltered_out": skipped,
            "api_calls": api_calls,
            "api_items": api_items,
            "classified": len(classified),
            "synthetic_items": synthetic_n,
        },
        "items": classified,
    }
    (Path(data_dir) / store.CLASSIFIED_NAME).write_text(json.dumps(out, indent=2))
    print(f"classified {len(classified)} items via {backend} "
          f"(archive={len(archive)}, api_calls={api_calls}, synthetic={synthetic_n}) "
          f"-> {store.CLASSIFIED_NAME}")
    if synthetic_n:
        print(f"  note: {synthetic_n} SYNTHETIC items present — verify_pipeline will "
              f"FAIL if this file ships as real data.", file=sys.stderr)


if __name__ == "__main__":
    main()
