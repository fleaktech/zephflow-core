# parser / fortinet-fortigate

**What it guards:** FortiGate traffic log key/value parsing (space-separated `key=value`, quoted values preserved).

**Source of input:** existing test resource `parser/fortinet_traffic_fortigate.txt` (whole file as one `__raw__` record). Mirrors the unit test `KvPairsExtractionRuleTest.extract`.

**Expected reviewed by:** _pending_ — carried over from `parser/fortinet_traffic_fortigate_parsed.json`. Re-review when changing `config.json`.
