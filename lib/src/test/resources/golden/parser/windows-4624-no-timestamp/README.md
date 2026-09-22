# parser / windows-4624-no-timestamp

**What it guards:** Windows 4624 multiline event parsing with no timestamp extraction.

**Source of input:** existing test resource `parser/windows_multiline_4624_no_timestamp.txt` (whole file as one `__raw__` record). Mirrors the unit test `WindowsMultilineExtractionRuleTest.testExtract_noTs`.

**Expected reviewed by:** _pending_ — carried over from `parser/windows_multiline_4624_no_timestamp_parsed.json`. Re-review when changing `config.json`.
