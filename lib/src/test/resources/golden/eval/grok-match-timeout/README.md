# eval / grok-match-timeout

**What it guards:** the `grok()` function stops at the 1 second match limit, fails only that record, and later records still evaluate.

**Source of input:** hand-written; same inputs as `parser/grok-match-timeout`.

**Time:** the match limit runs on the real clock because `MatcherWatchdog` takes no injected clock and this command is not `ClockAware`, so `_t` / `_advance` cannot apply. This fixture takes about 1 second.

**Expected reviewed by:** pending
