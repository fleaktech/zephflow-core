# eval / grok-unixpath-long-path

**What it guards:** the `grok()` function uses the same `%{UNIXPATH}` override as the parser. A long path that fails to match returns no result instead of a timeout error, so this fixture fails if the function goes back to the library default definition.

**Source of input:** same hand-written paths as `parser/grok-unixpath-long-path`.

**Expected reviewed by:** Claude, at Bo Lei's request, on 2026-11-04
