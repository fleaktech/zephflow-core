# sample / single-rule

**What it guards:** one rule `$.status == 200`, N=3, default tag field `__sampled__`, seed `GOLDEN_SEED = 42`.

- Every batch position gets kept once (3rd, then 1st, then 2nd), so neither "always first" nor
  "always last" passes.
- Each kept event is emitted when the 3rd event of its batch arrives. Batch 2's kept id 5 comes out
  after pass-through events 6, 7 and 8, which arrived later, so output order ≠ input order.
- A kept event that already has `__sampled__: 99` (id 4) gets 3. A pass-through event that has
  `__sampled__: 7` (id 8) is left alone.
- Kept events keep all other fields unchanged (nested `req` object and `tags` array on id 5, `status: 200.0` on
  id 12).
- The condition is strict equality: `"200"` (string), a missing `status`, 201 and 500 don't match
  and pass through untagged, right away. `200.0` does match.
- The trailing partial batch (ids 15, 16) is dropped, and output continues after it (id 17).

Trace (draws from one `new Random(42)`):

| id | rule / k | draw | output |
|---|---|---|---|
| 1 | r1 k=1 | — | |
| 2 | pass | | id 2 |
| 3 | r1 k=2 | nextInt(2)=1 | |
| 4 | r1 k=3 | nextInt(3)=0 → id 4 | id 4, `__sampled__` 3 |
| 5 | r1 k=1 | — | |
| 6, 7, 8 | pass | | ids 6, 7, 8 |
| 9 | r1 k=2 | nextInt(2)=1 | |
| 10 | r1 k=3 | nextInt(3)=2 | id 5, `__sampled__` 3 |
| 11 | r1 k=1 | — | |
| 12 | r1 k=2 | nextInt(2)=0 → id 12 | |
| 13 | pass | | id 13 |
| 14 | r1 k=3 | nextInt(3)=1 | id 12, `__sampled__` 3 |
| 15 | r1 k=1 | — | |
| 16 | r1 k=2 | nextInt(2)=0 | (partial, dropped) |
| 17 | pass | | id 17 |

**Expected reviewed by:** _pending_. `expected.jsonl` was derived from the FLE-2790 spec.
