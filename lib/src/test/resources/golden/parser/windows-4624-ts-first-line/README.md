# parser / windows-4624-ts-first-line

**What it guards:** Windows 4624 multiline event parsing with the timestamp taken from the first line.

**Source of input:** existing test resource `parser/windows_multiline_4624.txt` (whole file as one `__raw__` record). Mirrors the unit test `WindowsMultilineExtractionRuleTest.testExtract_tsFirstLine`.

**Expected reviewed by:** _pending_ — carried over from `parser/windows_multiline_4624_parsed.json`. Re-review when changing `config.json`.
