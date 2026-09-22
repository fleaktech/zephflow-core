# parser / panw-traffic

**What it guards:** syslog framing dispatching to the Palo Alto Networks traffic-log extractor on the `content` field, with the source field removed.

**Source of input:** existing test resource `parser/panw_traffic.txt` (whole file as one `__raw__` record). Mirrors the unit test `PanwTrafficExtractionRuleTest.test`.

**Expected reviewed by:** _pending_ — carried over from `parser/panw_traffic_parsed.json`. Re-review when changing `config.json`.
