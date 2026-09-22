# throttle / two-per-key-per-minute

**What it guards:** `allow 2 events per key per 60s` (`numToAllow > 1`), and specifically that
the period window is **anchored at the first event of the period**, not at the M-th allowed
event. The `one-per-key-per-minute` case cannot exercise this — at `numToAllow == 1` the two
anchorings coincide. This fixture makes the second allowed event arrive **late** within the
window, so a duty-cycle (anchor-at-M-th) implementation and the fixed-period (Cribl-style,
FLE-2792) implementation produce different output; it locks in the fixed-period behaviour.

Time is driven by `_t` (ms) in `input.jsonl` via the injected clock; nothing sleeps.

Sequence (host a, window = [first event, first event + 60s)):
- seq1 t=0 → passes, opens window [0, 60000), allowed 1/2
- seq2 t=40000 → passes (still in window), allowed 2/2 — arrives late on purpose
- seq3 t=50000 → dropped (over M within the window), dropped 1
- host b seq4 t=55000 → passes (independent key)
- seq5 t=61000 → window [0,60000) has ended, so this **opens a new window** and passes,
  stamped `throttledCount: 1`. A duty cycle anchored at seq2 would still be suppressing
  until 100000 and would drop seq5.
- seq6 t=61000 → passes, allowed 2/2 of window [61000,121000)
- seq7 t=61000 → dropped, dropped 1
- `_advance` 60s → seq8 t=121000 → new window, passes, stamped `throttledCount: 1`

**Expected reviewed by:** _pending_ — `expected.jsonl` was generated with
`-Dgolden.update=true` after implementing the fixed-period (first-event-anchored) semantics
per FLE-2792. A human must confirm it matches the intended Cribl-style behaviour before merging.

**Known non-goals:** does not cover the fail-open path (unkeyable events) or `cacheSizeLimit`
eviction — add separate cases for those.
