# Synthetic test fixtures — NOT real data

Every `.xml` file in this directory is **hand-authored fiction**. The headlines,
URLs, dates, and outlet content are invented for the sole purpose of unit-testing
the parser, dedup, classification, and aggregation logic offline (no network, no
API key).

Rules enforced by the pipeline:

- Each fixture channel carries `<generator>synthetic-test-fixture</generator>`.
- `fetch_feeds.py` stamps every item collected from a fixture with
  `"synthetic": true`.
- That flag rides through classification and rollup.
- `verify_pipeline.py` **FAILS** (check S2) if any `synthetic: true` item ever
  appears in the shipped `items_classified.json` or `media_history.json`.

So these files can never be mistaken for, or silently promoted into, real
collected data. They exist only to make `tests/test_pipeline.py` runnable and
deterministic. The URLs do not resolve; do not treat any content here as factual.
