# parser / windows-4672-eventviewer

**What it guards:** Windows 4672 event from Windows Event Viewer, including list-valued fields; timestamp read from `Date`.

**Source of input:** existing test resource `parser/windows_multiline_4672_windows_eventviewer.txt` (whole file as one `__raw__` record). Mirrors the unit test `WindowsMultilineExtractionRuleTest.testExtract_windowsEventViewer_List`.

**Expected reviewed by:** _pending_ — carried over from `parser/windows_multiline_4672_windows_eventviewer_parsed.json`. Re-review when changing `config.json`.
