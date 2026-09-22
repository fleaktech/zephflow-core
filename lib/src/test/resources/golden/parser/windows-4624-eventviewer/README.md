# parser / windows-4624-eventviewer

**What it guards:** Windows 4624 event copied from Windows Event Viewer (Date field), timestamp read from `Date`.

**Source of input:** existing test resource `parser/windows_multiline_4624_windows_eventviewer.txt` (whole file as one `__raw__` record). Mirrors the unit test `WindowsMultilineExtractionRuleTest.testExtract_windowsEventViewer`.

**Expected reviewed by:** _pending_ — carried over from `parser/windows_multiline_4624_windows_eventviewer_parsed.json`. Re-review when changing `config.json`.
