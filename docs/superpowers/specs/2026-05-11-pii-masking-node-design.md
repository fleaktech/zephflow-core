# PII Masking Node — Design

**Date:** 2026-05-11
**Status:** Approved for implementation
**Scope:** Candidate A from the PII Detection & Masking framing doc — minimal regex-based "Mask PII" node, shipped as a probe.

## Goal

Add an intermediate DAG node, `piimask`, that detects common structured PII in
configured fields of a `RecordFleakData` and replaces matches in place with a
configurable token. Ship the smallest credible thing that gives Gruve and
GTM something to evaluate against the "AI-Powered PII Detection / Auto-Masking"
assessment item. Defer ML/NER, tokenization, external integrations, and
policy infrastructure.

## Non-goals

- ML-based or contextual PII detection (Candidate B+).
- Format-preserving tokenization, reversible encryption, or hashing.
- External service integration (AWS Macie, Google DLP, Presidio, Skyflow).
- Pipeline- or org-level PII policy enforcement (Candidate D).
- Auto-scanning every string field in a record (operator enumerates targets).
- Recursive descent into nested records (operator enumerates leaf paths).
- Per-detector telemetry (single aggregate `pii_match_count`).

## Architecture

The node follows the existing intermediate-command pattern (modeled on
`eval`). New package: `lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/`.

| File | Role |
|---|---|
| `PiiMaskCommandDto.java` | Config records. |
| `PiiMaskConfigParser.java` | `Map<String,Object>` → DTO. |
| `PiiMaskConfigValidator.java` | Compile regexes and path expressions at DAG compile time. |
| `PiiMaskExecutionContext.java` | Holds compiled patterns, compiled paths, replacements, counters. |
| `PiiMaskCommand.java` | Extends `ScalarCommand`. |
| `PiiMaskCommandFactory.java` | Returns `CommandType.INTERMEDIATE_COMMAND`. |
| `PiiPathWriter.java` | Internal helper for in-place leaf rewrites by path. Scoped to this package. |

Registration:
- `MiscUtils.COMMAND_NAME_PII_MASK = "piimask"`.
- `OperatorCommandRegistry.OPERATOR_COMMANDS` gains a `piimask` entry.
- `ZephFlow.piiMask(PiiMaskCommandDto.Config)` fluent helper in `sdk/`.

Tests: `lib/src/test/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskCommandTest.java`.

## Configuration

```yaml
targets:                # required, non-empty list of JSON-path expressions
  - "$.event.message"
  - "$.user.email"
  - "$.recipients"      # array of strings → each element scanned
detectors:              # optional; presence of a sub-block enables that detector
  email:    { replacement: "[EMAIL]" }
  phone:    { replacement: "[PHONE]" }
  ssn:      { replacement: "[SSN]" }
  creditCard: { replacement: "[CC]" }
  ipv4:     { replacement: "[IPV4]" }
  ipv6:     { replacement: "[IPV6]" }
customPatterns:         # optional; each entry requires all three fields
  - name: "internal-id"
    pattern: "INT-\\d{8}"
    replacement: "[INTERNAL-ID]"
```

DTO shape:

```java
public interface PiiMaskCommandDto {
  record Config(
      List<String> targets,
      Detectors detectors,
      List<CustomPattern> customPatterns
  ) implements CommandConfig {}

  record Detectors(
      DetectorConfig email,
      DetectorConfig phone,
      DetectorConfig ssn,
      DetectorConfig creditCard,
      DetectorConfig ipv4,
      DetectorConfig ipv6
  ) {}

  record DetectorConfig(String replacement) {}

  record CustomPattern(String name, String pattern, String replacement) {}
}
```

### Parsing & validation rules

Enforced by `PiiMaskConfigParser` and `PiiMaskConfigValidator` at DAG compile
time. Misconfiguration must fail before execution begins.

1. `targets` is required and must be a non-empty list of strings.
2. Each target must be a simple dotted path:
   `^\$(\.[A-Za-z_][A-Za-z0-9_]*)+$`. Wildcards, array indexing (`[0]`),
   and bracket-quoted keys are out of scope for v1. This subset is what
   the writer can walk safely and is the only shape the operator-facing
   docs need to describe. The validator parses each target into a list
   of field-name segments at DAG compile time.
3. `detectors` is optional. A built-in is "active" iff its sub-block is present
   in the parsed map. A present block with `replacement == null` falls back to
   the built-in default token.
4. `customPatterns` is optional. Each entry requires non-blank `name`,
   `pattern`, `replacement`. `name` values must be unique within the list.
   `pattern` must compile as a `java.util.regex.Pattern`.
5. At least one detector must be active OR `customPatterns` must be non-empty.
   Otherwise the validator rejects the config with
   "PII mask node has no active detectors".

### Default replacement tokens

When a built-in detector's `replacement` is omitted: `[EMAIL]`, `[PHONE]`,
`[SSN]`, `[CC]`, `[IPV4]`, `[IPV6]`.

## Detection

Built-in patterns, compiled once in `PiiMaskExecutionContext`:

| Detector | Pattern | Post-filter |
|---|---|---|
| Email | `[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}` | — |
| Phone | US `(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}` combined with E.164 `\+[1-9]\d{1,14}` | — |
| SSN | Dashed `\b\d{3}-\d{2}-\d{4}\b` combined with no-dash `\b(?!000\|666)[0-8]\d{2}(?!00)\d{2}(?!0000)\d{4}\b` | — |
| Credit card | `\b(?:\d[ -]*?){13,19}\b` | Strip separators; Luhn checksum; match kept only on Luhn pass. |
| IPv4 | `\b(?:\d{1,3}\.){3}\d{1,3}\b` | Each octet validated 0–255. |
| IPv6 | Standard compressed/uncompressed form regex | — |

Custom patterns: compiled once in `PiiMaskExecutionContext` alongside the
built-ins. Stored with their `name` and `replacement`.

## Masking algorithm

`PiiMaskCommand` extends `ScalarCommand` and implements `processOneEvent`.

For each event:

1. Call `event.copy()` to get a mutable working copy (matches the existing
   `copyAndMerge` pattern; protects upstream references from rewrite).
2. For each path in `targets`:
   1. Resolve the path against the copy via `PathExpression`.
   2. If unresolved → skip silently.
   3. If resolved value is `StringPrimitiveFleakData` → mask the string; write
      the masked string back to the same path via `PiiPathWriter`.
   4. If resolved value is `ArrayFleakData` → iterate elements; for each
      string element, mask and replace in place. Non-string elements skipped.
   5. Any other type (record, number, boolean) → skip silently.
3. Return the (possibly mutated) copy as a single-element output list.

### Masking a single string

Apply detectors in this fixed order:

1. Custom patterns, in declared order.
2. Built-in detectors, in declared order: email, phone, ssn, creditCard,
   ipv4, ipv6.

Each detector runs once over the current string and fully consumes its
matches before the next detector runs. Run `matcher.replaceAll(...)` (or,
for credit card, `Matcher.results()` with manual rebuild so Luhn-failing
matches are preserved). Increment `pii_match_count` once per replacement
performed. After a replacement, the masked token (`[EMAIL]`, etc.) is opaque
to later detectors — the bracketed literals don't match any built-in pattern
and operators can pick custom replacements that don't either.

### Path resolution & write-back

Since targets are constrained to simple dotted paths, we sidestep the
ANTLR-backed `PathExpression` for the hot path and implement both read
and write as direct walks over `RecordFleakData.payload`:

- `PiiPathWriter` (internal to the `piimask` package) accepts a parsed
  list of field-name segments and the working `RecordFleakData`. It
  walks segments, dereferencing nested `RecordFleakData` payloads. If a
  segment is missing or the value isn't a `RecordFleakData` at an
  intermediate step, it returns without changes.
- At the leaf segment, if the value is a `StringPrimitiveFleakData` it
  is replaced. If the value is an `ArrayFleakData`, string elements are
  replaced in place; non-string elements are left untouched.
- The validator still parses each target via the dotted-path regex and
  rejects malformed input at DAG compile time.

## Error handling

Wrapped by the `ScalarCommand.process()` try/catch already in place:

- Unresolved path / non-string / non-array value → skip silently.
- Empty string value → no matches, no rewrite, no error.
- Path resolves to a nested record → skip silently (operator must enumerate
  leaf paths).
- Custom regex throws at runtime (e.g., catastrophic backtracking) → exception
  propagates; `ScalarCommand` catches and routes the event to `failureEvents` /
  DLQ with the exception message. Pattern compilation already happens at
  DAG compile time, so syntax errors never reach runtime.
- Overlapping matches across detectors → first detector to match wins
  (custom patterns first, then built-ins in declared order). Documented in
  the DTO comment.
- Credit-card false positives on long digit runs → Luhn filter handles.
  Matches that fail Luhn are left in place.

`RecordFleakData` is mutable (HashMap-backed payload) but the runner passes
the same record reference between nodes. We call `event.copy()` once at the
start of `processOneEvent` and rewrite into that copy, matching the
`copyAndMerge` convention. This protects upstream and parallel branches from
seeing redacted values they did not request.

## Telemetry

- Standard `DefaultExecutionContext` counters: input, output, error — per
  node, automatic, tagged with calling user via `basicCommandMetricTags`.
- One extra counter: `pii_match_count`, registered in
  `PiiMaskExecutionContext`, same tag scheme. Incremented once per
  replacement performed.

## Testing

`PiiMaskCommandTest` covers:

1. Each built-in detector matches and replaces with its default token.
2. Each built-in detector with a custom replacement string.
3. Disabled built-in (absent from config) → not replaced.
4. Custom pattern matches with custom replacement.
5. Multiple targets in one config; matches in each.
6. Target path missing → record passes through untouched.
7. Target path resolves to a non-string scalar → skipped.
8. Target path resolves to an array of strings → each element masked.
9. Mixed array (strings + numbers) → only strings masked.
10. Credit-card Luhn: valid number replaced, invalid 16-digit string intact.
11. IPv4 octet validation: `999.1.1.1` not replaced.
12. Validator rejects: empty `targets`, no active detectors, blank custom
    pattern name, invalid regex, duplicate custom-pattern names.
13. `pii_match_count` increments by the number of replacements made.
14. SDK fluent method: `ZephFlow.piiMask(config)` builds the node and processes
    a record end-to-end.

Test data uses inline JSON strings (matching `EvalCommandTest`'s pattern).
No new resource files.

## Out of scope (defer to a future iteration)

These are intentionally excluded to keep the build at ~1 week:

- ML/NER-based contextual PII (names, locations, addresses without
  structure).
- Format-preserving tokenization, hashing, reversible encryption.
- Auto-scan of all string fields; recursive descent into nested records.
- Per-detector telemetry; pipeline-summary reporting.
- External service backends (Macie, DLP, Presidio, Skyflow).
- Policy-level enforcement (Candidate D).
- Internationalized phone formats beyond US + E.164; country-specific
  national-ID detectors.
