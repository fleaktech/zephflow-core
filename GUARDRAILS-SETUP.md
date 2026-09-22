# One-time setup for the golden guardrails (zephflow-core, GitHub)

1. Create the two labels once:
       gh label create "golden: behaviour change"  --color D93F0B --description "PR changes a golden expected file; review the diff as a behaviour change"
       gh label create "golden: removal approved"  --color 0E8A16 --description "Maintainer approved deleting/renaming golden fixtures; reason in PR description"

2. Create the reviewer team (or replace the team in CODEOWNERS with usernames):
       Org settings → Teams → New team: golden-reviewers

3. Copy in:  .github/workflows/golden.yml  and  .github/CODEOWNERS

4. Branch protection on `main` (Settings → Branches → main):
   - Require status checks to pass → add "golden" as REQUIRED
   - Require a pull request before merging → tick "Require review from Code Owners"
   - Do not allow bypassing the above settings (otherwise admins skip it)

5. Sanity-check each guardrail once:
   - open a PR that edits one line of any expected.jsonl → label appears, code-owner review requested
   - open a PR that deletes a fixture directory → job fails; add "golden: removal approved" → job passes
   - open a PR that adds a fixture → nothing special happens
