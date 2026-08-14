# Next steps — two short things to run on your Mac

Everything is built and tested. Two git actions can't be done from the sandbox
(it can't remove git lock files on these mounts), so run them yourself. Both are
safe and specific.

---

## 1. Finish this project's second commit

The initial commit landed (all 23 files). A follow-up commit — 13 feeds marked
live-validated + the honest empty-state `verification_report.json` — is staged
but blocked by a stale lock.

```bash
cd ~/State\ Media\ Analysis
rm -f .git/HEAD.lock .git/index.lock
git add -A
git commit -m "Validate 13 feeds live; honest verify on empty state"
```

Then (optional) create a **new, separate** GitHub repo and push:

```bash
# create an empty repo named e.g. state-media-analysis on GitHub first, then:
git remote add origin https://github.com/<you>/state-media-analysis.git
git push -u origin main
```

To activate collection: the two workflows in `.github/workflows/` run as-is once
pushed. Add an `ANTHROPIC_API_KEY` repo secret if you want model-based
classification (it runs fine without one, in keyword mode).

Run the first real ingest whenever you like — locally is fine:

```bash
pip install -r requirements.txt
./run_ingest.sh        # fills items_archive.jsonl with REAL articles
./run_rollup.sh        # classify + snapshot + verify
open dashboard.html
```

---

## 2. Neutralize the fabricated data on the electoral-dashboard repo

The earlier mistake's fabricated `state-media-pipeline/` folder and an activated
`media-pipeline.yml` workflow are still on `origin/main` of
`ppothiawala-afk/electoral-dashboard`. This removes both, forward (no history
rewrite), and leaves all your electoral/Ghost work untouched:

```bash
cd ~/Documents/Claude/Projects/Electoral\ Dashboard
rm -f .git/index.lock .git/HEAD.lock

git rm -rf state-media-pipeline
git rm -f .github/workflows/media-pipeline.yml
rm -rf state-media-pipeline          # clear untracked leftovers (archive_store.py, tests/, __probe.txt)

git commit -m "Neutralize fabricated state-media-pipeline; deactivate its workflow

The state-media work now lives in its own standalone project. Removes the
fabricated fixtures + derived data and the activated media-pipeline.yml cron so
nothing fabricated remains public. Electoral pipeline untouched."

git push origin main
```

Your uncommitted electoral changes (`weekly-apply.yml`, `MANUAL_STEPS.md`,
`requirements.txt`, `WEBSITE_PLAN.md`, `publish_to_ghost.py`) stay untouched and
unstaged — this commit only removes the fabricated pipeline. Commit those
separately whenever that work is ready.

After the push, check the repo's Actions tab: the `state-media-pipeline` /
`media-pipeline` workflow should no longer be listed.
