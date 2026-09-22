# throttle / one-per-key-per-minute

**What it guards:** `allow 1 event per key per 60s`, including the period boundary — which
the existing `ThrottleCommandTest` does not exercise across a real 60s window.

Time is driven by `_t` (ms) in `input.jsonl` via the injected clock; nothing sleeps.

Sequence: host a at t=0, 100, 200 (only the first passes) → host b at t=250 (passes) →
host a at t=60000 (new period: passes, stamped `throttledCount: 2`) → host a at t=60050
(dropped) → `_advance` 60s → host a at t=120050 (passes, `throttledCount: 1`).

**Expected reviewed by:** _pending_ — `expected.jsonl` below was written by hand from the
command's documented semantics. Run once with `-Dgolden.update=true`, diff against this
file, and record the review here before merging.
