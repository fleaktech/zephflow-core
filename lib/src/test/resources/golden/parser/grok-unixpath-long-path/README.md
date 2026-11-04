# parser / grok-unixpath-long-path

**What it guards:** `%{UNIXPATH}` captures paths as before, and a long path that fails to match reports a plain no-match error instead of a timeout. The library default definition is exponential on this input and hits the match limit, so this fixture fails if the override is lost.

**Source of input:** hand-written paths.

**Expected reviewed by:** pending
