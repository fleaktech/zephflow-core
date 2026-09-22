# zephflow-core — agent guidance

## Golden fixture tests (lib/src/test/resources/golden/)

Golden currently lives only in the `lib` module because every ScalarCommand (the only golden
target) lives there. The harness, the `goldenTest` task, the `golden.yml` `GOLDEN_DIR`, and the
CODEOWNERS globs are all scoped to `lib/`. If another module ever gains a ScalarCommand that needs
golden coverage, you must extend those four things to that module too — a fixture placed elsewhere
is NOT run or guarded automatically.

- Every ScalarCommand change must keep `./gradlew :lib:goldenTest` green.
- A failure prints `output[i].field: expected=… actual=…`. Read it, fix the CODE.
- NEVER edit expected.jsonl or expected_errors.jsonl by hand and NEVER run
  `-Dgolden.update=true` unless the ticket explicitly says the output must change.
  If it does: run update, then `git diff` the expected file and explain every
  changed line in the PR description. The update run fails on purpose.
- New behaviour ⇒ add a new fixture dir (README.md + config.json + input.jsonl),
  generate expected with update mode, and leave "Expected reviewed by: pending"
  in README.md for a human to fill in.
- Do not add fields to any ignore list, do not @Disabled a golden test, do not
  delete or rename fixture directories.
- Time: use `_t` / `_advance` control records in input.jsonl. Never Thread.sleep.
