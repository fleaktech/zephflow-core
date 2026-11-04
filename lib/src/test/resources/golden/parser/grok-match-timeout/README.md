# parser / grok-match-timeout

**What it guards:** a grok expression that backtracks without bound on a failed match stops at the 1 second match limit, fails only that record, and later records still parse.

**Source of input:** hand-written; the second record is 500 words with no `END`, which makes five `GREEDYDATA` fields backtrack far past the limit.

**Time:** the match limit runs on the real clock because `MatcherWatchdog` takes no injected clock and this command is not `ClockAware`, so `_t` / `_advance` cannot apply. This fixture takes about 1 second.

**Expected reviewed by:** pending
