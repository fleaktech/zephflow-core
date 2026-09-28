# sample / end-of-input-incomplete-group

**What it guards:** at end of input each rule's incomplete group emits its picked event, tagged
with the group's actual size (not the configured rate), and the rule's state is reset afterwards.
Seed `GOLDEN_SEED = 42`. Default field `__sampled__`. Rules:

1. `$.level == "info"`, N=3
2. `$.level == "warn"`, N=4

- **Full group** (ids 1, 2, 3) emits id 3 with `__sampled__: 3` when id 3 arrives.
- **Pass-through:** id 7 matches no rule.
- **First `_end_of_input`:**
  - rule 1's incomplete group (ids 4, 6; id 4 picked) emits id 4 with `__sampled__: 2`
  - rule 2's incomplete group (id 5) emits id 5 with `__sampled__: 1`
- **Reset:** after the flush, id 8 opens a new rule-1 group. The second `_end_of_input` emits it
  with `__sampled__: 1`.
- **Nothing pending:** the third `_end_of_input` emits nothing.

The tags sum to 7 (3 + 2 + 1 + 1), the number of matched events (ids 1–6 and 8).

**Expected reviewed by:** _pending_.
