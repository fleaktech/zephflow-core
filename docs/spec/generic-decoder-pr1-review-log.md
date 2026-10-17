# PR1 code review log

Spec: `docs/spec/generic-decoder.md`. P-numbers refer to its section 8.

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
| A9 | medium | accepted as P8 (line framing never uses the budget flag; budget outcomes are terminal-only) | spec §8 |
| A10 | medium | accepted: deviations P3–P13 recorded | spec §8 |
| A11 | low | accepted: text whole exact-limit test | TextDecodeTest |
| A12 | low | accepted: unknown names assert UncheckedIOException caused by JsonMappingException | DecodingConfigCompilerTest |
| A13 / B9 | low | accepted: whitespace-only array input → MALFORMED | JsonArrayFormat; test `whitespaceOnlyInputIsMalformed` |
| A14 | medium (scope) | accepted as deviation P9 (kept) | spec §8 |
| A15 | medium (scope) | accepted as deviation P11 (kept) | spec §8 |
| A16 | low (scope) | accepted: blank definition recorded P10 | spec §8 |
| B2 | medium | accepted: budget comparison rewritten to avoid overflow | BudgetCaptureStream; test `hugeLimitsDoNotOverflow` |
| B6 | low | deferred: whitespace > 64 KiB between array elements counts toward the next element (P13) | — |
| B7 | low | accepted: header cap raised to 1 MiB (P12); XLEN=65535 header test | GzipDecodeTest `headerWithEveryOptionalFieldDecodes` |
| B8 | low | accepted: GuardedCursor unit tests incl. wrapped read failure | GuardedCursorTest |

## Round 2 — 396de28
Verdicts: A = CHANGES, B = CHANGES. All round-1 accepted findings FIXED except A6 (re-raised as R2-B2/A5).

| ID | Sev | Disposition | Where fixed / evidence |
|---|---|---|---|
| B1 | blocker | accepted: own `Utf8TranscodingStream` replaces commons-io ReaderInputStream (one source read per returned chunk) | internal/Utf8TranscodingStream; JsonLinesDecodeTest `utf16ReadFailure…`, `utf16GzipWithACutTrailer…` |
| A1 | high | accepted: shell reclassifies terminal outcomes via flags (P14) | GuardedCursor; GuardedCursorTest `aTerminalOutcomeAfterADecompressFailure…` |
| B2 / A2 / A5 | high / medium / low | accepted: expected counts derived from the input (byte-at-a-time inflater), exact boundary counts | GzipDecodeTest `truncatedBody…`, `networkFailureAnywhere…` |
| B3 | medium | accepted: RFC 4627 4-byte patterns + BOM; decide UTF-8 from two non-zero bytes | JsonEncoding; test `strayNulAtTheStartOfUtf8…` |
| B4 | medium | accepted: only the malformed code unit is replaced | Utf8TranscodingStream; test `loneUtf16Surrogate…` |
| B5 | low | accepted: peek reads only what is needed; new PeekedStream never reads the source in the same call that replays peeked bytes (JDK PushbackInputStream does, losing them on failure — also fixed in auto-detect) | internal/PeekedStream; test `shortFirstRecordComesOutBeforeAGzipFailure` |
| B6 | low | accepted: transcoded output drops the BOM | Utf8TranscodingStream |
| B7 | low | accepted: 1 ms pause between zero-byte retries | ReadGuardStream; test `aStreamThatOnlyReturnsZeroBytes…` |
| B8 | low | accepted: P6 wording (member records confirmed only by its CRC) | spec §8 |
| A3 | medium | accepted as P13 (wording widened) | spec §8 |
| A4 | medium | accepted as P15 | spec §8 |
| A6 | low | accepted: case-insensitivity recorded (P11); zero-read failure branch tested; no-progress guard recorded as untestable defensive code (P16) | spec §8; DecodingConfigCompilerTest |

## Round 3 (cap) — 719221b
Verdicts: A = CHANGES (medium/low only), B = CHANGES (1 blocker). All round-2 accepted findings FIXED.

| ID | Sev | Disposition | Where fixed / evidence |
|---|---|---|---|
| B1 | blocker | accepted; fixed after the cap under the user's standing instruction to keep going without asking — reviewed only by the confirmation round below. UTF-8 encoder replaces lone surrogates (UTF-32 D800–DFFF); a full char buffer no longer overflows on malformed input; `done` is never set while chars remain; a no-progress guard stops any spin | internal/Utf8TranscodingStream; JsonLinesDecodeTest `invalidUtf32CodePoints…` (D800, DC00, 0x110000, 5000 following records, 20 s timeout) |
| A1 | medium | accepted: BOM tests for UTF-16LE (lines), UTF-16BE (array), UTF-32LE (object), UTF-32BE without BOM. Adding them exposed a false "no progress" when a read returned only the BOM; the progress check now counts bytes read from the source | JsonLines/JsonArray/JsonObject tests |
| A2 | low | accepted: gzip-cut tests for json array (exact element count from the input) and json object (recordIndex -1) | JsonArrayDecodeTest, JsonObjectDecodeTest |
| A3 | low | accepted: char literals written as escapes | Utf8TranscodingStream |
| A4 | low | accepted: unused DecodeFlags.anySet and Previews.head removed | — |
| (B probe) | low | deferred: a UTF-32 surrogate pair written as two code points is combined into one character instead of two U+FFFD | — |

## Confirmation round — 03d75ab (diff 719221b...03d75ab)
Verdict: CLEAN. All five post-cap fixes FIXED (fuzz: 15,200 UTF-16/UTF-32 cases, 0 mismatches; the same fuzz hangs on 719221b).
Accepted medium/low findings, recorded and not fixed here (carried into PR2):
- RC-1 medium: JsonArrayDecodeTest gzip-cut test derives its expected count from the output; should use an independent inflater count like GzipDecodeTest.
- RC-2 low: Utf8TranscodingStream no-progress guard has no test and is not in spec §8.
- RC-3 low: P5 says every bad UTF-32 unit becomes its own U+FFFD, but D800+DC00 written as two code points is combined (deferred item).
- RC-4 low: UTF-32 with a BOM followed by a real U+FEFF loses that U+FEFF (JDK strips the BOM, then dropLeadingBom drops the next one).

## Result
Tier M, 3 rounds + confirmation. Round verdicts: R1 A=CHANGES B=CHANGES (a550001); R2 A=CHANGES B=CHANGES (396de28); R3 A=CHANGES (medium/low only) B=CHANGES (719221b); confirmation CLEAN (03d75ab).
Not converged inside the cap: R3-B1 (blocker) was fixed after the cap under the user's standing instruction to continue without asking, and was reviewed only by the confirmation round.
Not covered: Testcontainers-based tests could not run locally (Docker Hub credentials rejected); CI runs them.
