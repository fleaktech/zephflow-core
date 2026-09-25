# sample / multi-rule

**What it guards:** first-match precedence, independent per-rule batches, N = 1, and a custom
field on pass-through events. Seed `GOLDEN_SEED = 42`. `sampleRateField: "rate"`. Rules:

1. `$.path == "/health"`, N=2
2. `$.level == "debug"`, N=1
3. `$.status == 200 and $.ok`, N=3

- **First match wins.**
  - id 1 (health + 200 + ok) goes to rule 1, not rule 3.
  - id 2 (debug + 200 + ok) goes to rule 2, not rule 3.
  - id 6 (debug + health) goes to rule 1, not rule 2.
- **N = 1** (ids 2, 8): emitted immediately with `rate: 1` and **no draw**. A draw here would
  shift rule 1's pick (id 6 kept instead of id 1), so the expected output would differ.
- **The last rule's condition doesn't match, so the event passes through:**
  - id 4: `and` over non-boolean `ok: 1` throws → pass through; no error, doesn't join the batch.
  - id 9: `ok` missing → `and` is false → pass through.
  - ids 5, 13: pass through. id 5 already has `rate: 7` and `__sampled__: 4`; both are left alone
    (a pass-through event is never tagged).
- **Kept event goes out later than it arrived:** rule 1's id 1 is emitted when id 6 arrives.
  Rule 3 keeps its 3rd event (id 10).
- **Partial batches** of rule 1 (id 11) and rule 3 (id 12) are dropped at the end.

Trace:

| id | rule / k | draw | output |
|---|---|---|---|
| 1 | r1 k=1 | — | |
| 2 | r2 k=1 (N=1) | — | id 2, `rate` 1 |
| 3 | r3 k=1 | — | |
| 4 | pass (r3 throws) | | id 4 |
| 5 | pass | | id 5 |
| 6 | r1 k=2 | nextInt(2)=1 | id 1, `rate` 2 |
| 7 | r3 k=2 | nextInt(2)=0 → id 7 | |
| 8 | r2 k=1 (N=1) | — | id 8, `rate` 1 |
| 9 | pass | | id 9 |
| 10 | r3 k=3 | nextInt(3)=0 → id 10 | id 10, `rate` 3 |
| 11 | r1 k=1 | — | (partial, dropped) |
| 12 | r3 k=1 | — | (partial, dropped) |
| 13 | pass | | id 13 |

**Expected reviewed by:** _pending_. `expected.jsonl` was derived from the FLE-2790 spec.
