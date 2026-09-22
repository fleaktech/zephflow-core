# parser / windows-4624-ts-from-field

**What it guards:** Windows 4624 multiline event parsing with the timestamp read from the `Date` field.

**Source of input:** existing test resource `parser/windows_multiline_4624_ts_in_field.txt` (whole file as one `__raw__` record). Mirrors the unit test `WindowsMultilineExtractionRuleTest.testExtract_tsFromField`.

**Expected reviewed by:** _pending_ — carried over from `parser/windows_multiline_4624_parsed_ts_in_field.json`. Re-review when changing `config.json`.
