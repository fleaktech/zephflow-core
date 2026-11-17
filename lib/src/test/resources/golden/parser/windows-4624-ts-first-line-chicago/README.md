# parser / windows-4624-ts-first-line-chicago

**What it guards:** Windows 4624 multiline event parsing with the timestamp taken from the first line and interpreted in the configured `timezone` (`America/Chicago`), so `__ts__` is the epoch-millisecond number for 2021-03-12T16:48:14Z rather than the UTC reading of the same text.

**Source of input:** same input as `windows-4624-ts-first-line` (existing test resource `parser/windows_multiline_4624.txt`, whole file as one `__raw__` record). Mirrors the unit test `WindowsMultilineExtractionRuleTest.testCompiledConfig_namedTimezone`.

**Expected reviewed by:** pending
