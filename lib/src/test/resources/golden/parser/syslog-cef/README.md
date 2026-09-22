# parser / syslog-cef

**What it guards:** syslog framing (priority/timestamp/device) dispatching to a CEF extractor on the `content` field, with the source field removed.

**Source of input:** existing test resource `parser/syslog_cef_1.txt` (whole file as one `__raw__` record). Mirrors the unit test `SyslogExtractionRuleTest.extract`.

**Expected reviewed by:** _pending_ — carried over from `parser/syslog_cef_1_parsed.json`. Re-review when changing `config.json`.
