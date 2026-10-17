# PR1 code review log

Tier M (A+B, cap 3). Mode: code. Base bcda85e (feature/generic-decoder).

## Round 1 — a550001
Verdicts: A = CHANGES, B = CHANGES.

| ID | Sev | Disposition | Where fixed / evidence |
|---|---|---|---|
| A1 / B5 | blocker / medium | accepted: non-UTF-8 JSON is transcoded to UTF-8 (RFC 4627 §3 detection) instead of rejected | JsonEncoding; JsonLines/Array/Object tests `utf16*` |
| B1 | blocker | accepted: bytes inflated before corrupt data are recovered via `Inflater.getBytesWritten()` (JDK 21 Inflater.java:409-415 adds them before rethrow) | GzipMemberStream inflate; GzipDecodeTest `corruptDeflateDataMidMember…` |
| A2 | high | accepted: header-limit test now counts bytes read and checks the message | GzipDecodeTest `unterminatedFileName…` |
| A3 | high | accepted: views compare error class + first message line and preview content | DecodeTestSupport |
| A4 | high | accepted: tests for >20M string, >50k name (Decoded) and depth 1001 (MALFORMED) | JsonLinesDecodeTest |
| A5 / B4 | high / medium | accepted: mid-body corruption, garbage between members, all-optional-header-fields tests; dictionary branch removed (impossible with nowrap) | GzipDecodeTest |
| A6 | medium | accepted: cuts inside a deflate body; whole outcome list compared | GzipDecodeTest `networkFailureAnywhere…` |
| A7 | medium | accepted: after a read failure every later next() throws again | GuardedCursor; DecodingConfigCompilerTest `readFailureIsNotFollowedByACleanEnd` |
| A8 / B3 | medium | accepted: ReadGuardStream retries 0-byte reads (up to 1000), then fails as a read failure | ReadGuardStream; test `zeroByteReadsAreRetried…` |
| A9 | medium | accepted as plan deviation P8 (line framing never uses the budget flag; budget outcomes are terminal-only) | plan §7 |
| A10 | medium | accepted: deviations P3–P13 recorded | plan §7; design doc §4.1/§5.1/§5.2 updated |
| A11 | low | accepted: text whole exact-limit test | TextDecodeTest |
| A12 | low | accepted: unknown names assert UncheckedIOException caused by JsonMappingException | DecodingConfigCompilerTest |
| A13 / B9 | low | accepted: whitespace-only array input → MALFORMED | JsonArrayFormat; test `whitespaceOnlyInputIsMalformed` |
| A14 | medium (scope) | accepted as deviation P9 (kept) | plan §7 |
| A15 | medium (scope) | accepted as deviation P11 (kept) | plan §7 |
| A16 | low (scope) | accepted: blank definition recorded P10 | plan §7 |
| B2 | medium | accepted: budget comparison rewritten to avoid overflow | BudgetCaptureStream; test `hugeLimitsDoNotOverflow` |
| B6 | low | deferred: whitespace > 64 KiB between array elements counts toward the next element (plan P13) | — |
| B7 | low | accepted: header cap raised to 1 MiB (P12); XLEN=65535 header test | GzipDecodeTest `headerWithEveryOptionalFieldDecodes` |
| B8 | low | accepted: GuardedCursor unit tests incl. wrapped read failure | GuardedCursorTest |
