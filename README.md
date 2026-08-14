# State Media Analysis

A scheduled pipeline that collects public state-level news RSS feeds across all
50 states, classifies each article by topic, and tracks how coverage of four
topics shifts over time: **climate, politics, economic development, and
environmental legislation**.

It captures and counts coverage. **It does not infer sentiment, tone, or bias** —
every number on the dashboard is a countable fact traceable to a source article.

## Status (2026-08-14)

**No real data has been collected yet.** This repository ships with the pipeline,
a 50-state feed registry, tests, and an empty-state dashboard — but zero articles.
Week one is genuinely week one. The dashboard shows an honest "no data yet" state
and the source registry until the first ingest runs.

## Architecture — one archive, three products

The design principle is that the **article-level archive is the only durable
asset**, and everything else is a disposable view derived from it.

```
feeds_config.json ──▶ fetch_feeds.py ──▶ items_archive.jsonl   (append-only, the source of truth)
   (registry)          (daily, cheap)         │
                                              ├▶ classify.py ──▶ items_classified.json   (rebuildable)
                                              │    (weekly)
                                              └▶ append_history.py ──▶ media_history.json (the lookback product)
                                                   (weekly)              │
                                   verify_pipeline.py ◀──────────────────┘  (checks that can FAIL)
                                              │
                                        dashboard.html   (static, reads the JSON)
```

Why append-only: RSS feeds are a moving window (10–50 recent items; older ones
vanish). A weekly aggregate is lossy — you can't get back from "CO had 4 climate
items" to *which* four. Keeping every article means we can re-classify history
when the topic rubric changes, and later build entity-level share-of-voice and
cross-state narrative-propagation analyses that a weekly rollup would make
impossible. `items_archive.jsonl` is never rewritten or truncated;
`verify_pipeline.py` FAILS if it ever shrinks below its watermark.

## Cadence (split on purpose)

- **Daily — `run_ingest.sh`** (`.github/workflows/ingest.yml`): fetch feeds, append
  new items to the archive. No API, essentially free. Daily because feeds expire.
- **Weekly — `run_rollup.sh`** (`.github/workflows/rollup.yml`): classify the
  archive, append a coverage snapshot, verify, and run a feed health check.

## Metric: share first, volume second

Raw article counts conflate "this outlet publishes a lot" with "this topic is hot
here." The **primary metric is topic *share*** — each topic's fraction of a
source's (or state's) total classified output. Absolute volume is retained as a
secondary, drill-down measure. Snapshots also record outlet counts, so heavy
output from one prolific outlet is never mistaken for many outlets agreeing.

## Data provenance & limitations — read before trusting a number

- **Sources are one network, not a state's whole press.** The registry is
  predominantly the [States Newsroom](https://en.wikipedia.org/wiki/States_Newsroom)
  nonprofit network (39 affiliates) plus 11 partner nonprofits — a single,
  ideologically non-neutral publisher set with a documented progressive lean,
  especially relevant on climate and environmental topics. Coverage here reflects
  *their* editorial priorities. The dashboard's "Source composition" panel makes
  this visible rather than hiding it.
- **Feeds are unvalidated candidates until proven live.** Every `feed_url` is
  derived from the outlet's real name + the standard WordPress `/feed/`
  convention. **None has been live-validated in this project yet** —
  `validation.validated` is `false` on all 51 rows and `status` is `"candidate"`.
  The first `discover_feeds.py --health-check` confirms which resolve and proposes
  promoting them to `active`. No `feed_url` was invented; unconfirmed ones may 404.
- **Median feeds per state is low (≈1).** Growing to 3–5/state is the job of the
  human-gated discovery loop below.
- **No sentiment, by design.** Topic tags and entity mentions only. A structural
  check (`S1`) fails the build if any judgment key ever appears.

## Feed discovery & health (human-gated)

`discover_feeds.py` never edits the registry. It writes proposals to
`feeds_patch.json` (adds / validates / flags); a human reviews; `apply_feeds_patch.py`
merges approved entries and writes a dated audit copy. This is the answer to the
#1 failure mode of media monitoring — source rot — and the approval gate is the
point, not an inconvenience.

## Verification — checks that can actually fail

`verify_pipeline.py` labels each check **STRUCTURAL** (holds regardless of data
volume) or **DATA** (WARNs until real data exists, so an empty pipeline is never
mistaken for a verified one):

- `S1` no sentiment/judgment key anywhere · `S2` no synthetic-test data in shipped
  files · `S3` topics within the locked set · `S6` share = volume/total
- `D1` archive not truncated (watermark tripwire), no duplicate ids, all lines
  parse · `D2` sightings resolve to archive items · `D3` latest snapshot totals
  recomputed **independently from the archive window** (not circular) · `D4` a fed
  state silent for N snapshots is flagged as a likely dead feed · `D5` feed
  validation coverage

## Roadmap (earned by history, not promised now)

Predictive work is deliberately deferred until months of real snapshots exist,
and framed honestly as **anomaly detection**, not forecasting: flag a state/topic
deviating from its own trailing baseline; detect legislative-session seasonality;
(with the sightings log) measure cross-state story propagation. None of it is
credible without real history, which is the argument for starting ingestion early
and quietly.

## Setup

```bash
pip install -r requirements.txt          # feedparser (+ anthropic for API mode)
python3 -m unittest discover -s tests     # 9 offline tests, no network/key

./run_ingest.sh                           # collect (do this daily)
./run_rollup.sh                           # classify + snapshot + verify (weekly)
open dashboard.html                       # or serve the folder
```

Classification runs key-free by default (deterministic keyword tagging). Set
`ANTHROPIC_API_KEY` to use the model for better topic/entity extraction; only
keyword-pre-filter survivors are ever sent, so cost is bounded.

To activate CI: the two workflows in `.github/workflows/` are ready; push to a
GitHub repo and (optionally) add the `ANTHROPIC_API_KEY` secret.

## Tests

`python3 -m unittest discover -s tests` — drives the real scripts against the
clearly-labelled synthetic fixtures in `tests/fixtures_synthetic/` (invented
content, non-resolving URLs, flagged `synthetic:true` end-to-end) into a temp
dir. Covers dedup, idempotent re-ingest, deterministic rebuild, share math, and
three ways verification is proven to FAIL (synthetic leak, duplicate id,
truncation).
