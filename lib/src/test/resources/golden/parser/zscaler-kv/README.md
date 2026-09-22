# parser / zscaler-kv

**What it guards:** key/value extraction for Zscaler NSS feed logs — a single field `f`
holding a tab-separated `key=value` string, split with `pairSeparator="\t"` and
`kvSeparator="="`, with the source field removed (`removeTargetField: true`).

**Source of input:** existing test resource `parser/zscaler_kv_data.json` (one JSON record),
fed here as one line in `input.jsonl`. This mirrors `ParserCommandTest.testParseKvWithTabSeparator`,
which drives the real `ParserCommand` with `parser/zscaler_kv_config.json` on JSON_OBJECT input.

**Expected reviewed by:** _pending_ — carried over from `parser/zscaler_kv_parsed.json`, the
assertion target of `ParserCommandTest.testParseKvWithTabSeparator`. Re-review when changing the
KV separators or `config.json`.

**Known non-goals:** does not cover malformed KV input or quoting edge cases (see
`KvPairsExtractionRuleTest` for those unit-level cases).
