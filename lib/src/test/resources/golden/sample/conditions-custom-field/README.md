# sample / conditions-custom-field

**What it guards:** condition semantics (only boolean `true` matches; a throw means no match and
the walk continues, even after two throws in a row), a catch-all rule, an unreachable rule, and a
custom `sampleRateField`. Seed `GOLDEN_SEED = 42`. `sampleRateField: "rate"`. Rules:

1. `($.a / $.b) > 1`, N=2
2. `($.a % $.b) > 100`, N=2 (never true for this input; it's here to throw a second time)
3. `$.flag`, N=2
4. `$.code != 404`, N=2
5. no condition (catch-all), N=3
6. `$.x == 1`, N=1 (unreachable: rule 5 catches everything before it)

- **Throw → next rule, not a failure, twice in a row:**
  - id 2 (`b: 0`): rule 1 throws (divide by zero), rule 2 throws (mod by zero), rule 3 doesn't
    match, and it lands on rule 4.
  - id 3 (`a: "x"`): rules 1 and 2 both throw (type mismatch), and it lands on rule 3.
  - `expected_errors.jsonl` is absent, so there are no failure events.
- **Only boolean `true` matches** `$.flag`:
  - ids 3 and 8 (`true`) match.
  - id 4 (`1`), id 5 (`"true"`), id 6 (`[true]`) and id 11 (`false`) don't.
  - If id 6 matched, it would complete rule 3's batch with id 3 and change the output.
- **Null handling:**
  - id 11 has no `a`, so `null / 2` and `null % 2` give `null`, and `null > …` gives `null`: no
    match.
  - Missing `code` means `$.code != 404` is **true** (ids 2 and 10 match rule 4).
- **Catch-all and unreachable rule:**
  - ids 4, 5, 6 and 11 all go to rule 5. id 5 has `x: 1` but never reaches rule 6.
  - id 10 has `x: 1` but lands on rule 4.
- **Custom field:** kept events get `rate`.
  - id 1 already has `rate: 0`, which is overwritten with 2.
  - The default-named `__sampled__: 5` on ids 1 and 8 is kept unchanged.
- **Each rule gets its own batch,** and all rules draw from one shared generator. A per-rule
  generator would pick differently here.
- **Partial batches** of rule 4 (id 10) and rule 5 (id 11) are dropped.

Trace:

| id | rule / k | draw | output |
|---|---|---|---|
| 1 | r1 k=1 | — | |
| 2 | r4 k=1 (r1, r2 threw) | — | |
| 3 | r3 k=1 (r1, r2 threw) | — | |
| 4 | r5 k=1 | — | |
| 5 | r5 k=2 | nextInt(2)=1 | |
| 6 | r5 k=3 | nextInt(3)=0 → id 6 | id 6, rate 3 |
| 7 | r1 k=2 | nextInt(2)=1 | id 1, rate 2 |
| 8 | r3 k=2 | nextInt(2)=0 → id 8 | id 8, rate 2 |
| 9 | r4 k=2 | nextInt(2)=0 → id 9 | id 9, rate 2 |
| 10 | r4 k=1 | — | (partial, dropped) |
| 11 | r5 k=1 | — | (partial, dropped) |

**Expected reviewed by:** _pending_. `expected.jsonl` was derived from the FLE-2790 spec.
