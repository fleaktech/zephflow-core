# parser / cisco-asa-grok-dispatch

**What it guards:** the two-stage grok parse for Cisco ASA syslog — a header grok
(`%ASA-<level>-<message_number>`) followed by a per-message-number dispatch grok. Covers
message numbers 302013, 302014, 302015, 302016, 305011 (30 records, first 30 of the
original `parser/cisco_asa_data.txt`).

**Source of input:** existing test resource `parser/cisco_asa_data.txt`, lines 1–30.
Already anonymised (localhost, RFC-1918 / 100.64.0.0/10 addresses).

**Expected reviewed by:** _<name>_ on _<date>_ — carried over from `cisco_asa_parsed.json`,
which was the assertion target of `CompiledRulesTest.testParseCiscoAsaLog`. Re-review when
changing any grok in `config.json`.

**Known non-goals:** does not cover 106023 / 113019 / 113039 (add a second case for those).
