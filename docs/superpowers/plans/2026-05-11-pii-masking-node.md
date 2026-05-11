# PII Masking Node Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `piimask` intermediate command node that detects and replaces common structured PII (email, phone, SSN, credit card, IPv4, IPv6) plus user-defined regex patterns in operator-specified fields of a `RecordFleakData`.

**Architecture:** Follows the existing intermediate-command pattern (modeled on `eval`). Targets are simple dotted paths (`$.foo.bar`), so we walk `RecordFleakData.payload` directly rather than going through ANTLR-backed `PathExpression`. Detection is regex-based, with a Luhn post-filter for credit cards and per-octet validation for IPv4. The node rewrites string leaves in place — `RecordFleakData.copy()` is shallow and would not isolate nested leaves, so the probe accepts in-place mutation. Built-in detectors are opt-in (presence in config = enabled); each detector and each custom pattern carries its own replacement string.

**Tech Stack:** Java 17, Gradle, JUnit 5, Lombok, Jackson (via existing `OBJECT_MAPPER`), `java.util.regex`.

**Spec reference:** `docs/superpowers/specs/2026-05-11-pii-masking-node-design.md`

---

## File map

Files to create under `lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/`:

| File | Responsibility |
|---|---|
| `PiiMaskCommandDto.java` | `Config`, `Detectors`, `DetectorConfig`, `CustomPattern` records. |
| `DottedPath.java` | Record wrapping parsed path segments; static `parse(String)` + regex constant. |
| `PiiPathWriter.java` | Read/replace string leaves (or string elements of an array leaf) at a parsed path. |
| `BuiltInDetectors.java` | Enum of the six built-in detectors with patterns, default tokens, and any post-filter. |
| `PiiMasker.java` | Masks one string given a list of active detector specs; returns masked text + replacement count. Includes Luhn and IPv4 octet validation. |
| `PiiMaskConfigParser.java` | Map → DTO. |
| `PiiMaskConfigValidator.java` | Compile all patterns, parse all paths, enforce semantic rules. |
| `PiiMaskExecutionContext.java` | Holds compiled patterns, parsed paths, `pii_match_count` counter. |
| `PiiMaskCommand.java` | Extends `ScalarCommand`; per-event copy + mask + emit. |
| `PiiMaskCommandFactory.java` | Returns `INTERMEDIATE_COMMAND`. |

Files to create under `lib/src/test/java/io/fleak/zephflow/lib/commands/piimask/`:

| File | Coverage |
|---|---|
| `DottedPathTest.java` | Path parsing happy path + rejection of malformed input. |
| `PiiPathWriterTest.java` | Read + write at various path shapes including missing intermediate. |
| `BuiltInDetectorsTest.java` | Each pattern matches the right inputs and ignores the wrong ones. |
| `PiiMaskerTest.java` | Single-string masking, ordering, Luhn, IPv4 octet bounds, custom patterns. |
| `PiiMaskConfigParserTest.java` | Parsing presence-as-activation, custom-pattern shape, optional fields. |
| `PiiMaskConfigValidatorTest.java` | Validator rejections. |
| `PiiMaskCommandTest.java` | End-to-end: build the command via factory, run records, assert mutations + counters + SDK fluent method. |

Files to modify:

- `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java` — add `COMMAND_NAME_PII_MASK = "piimask"`.
- `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java` — register the factory.
- `sdk/src/main/java/io/fleak/zephflow/sdk/ZephFlow.java` — add `piiMask(PiiMaskCommandDto.Config)` fluent method.

---

## Conventions

- Test framework: JUnit 5 (`org.junit.jupiter.api.Test`).
- Apache 2.0 license header on every new `.java` file (copy from `EvalCommand.java`).
- Run tests via: `./gradlew :lib:test --tests '<fully.qualified.TestClass>'`
- Run full module tests via: `./gradlew :lib:test` and `./gradlew :sdk:test`.
- After each task, commit with `feat(piimask): <one-line summary>` (no Co-Authored-By unless you also want to credit the model). Stage only the files the task touched.

---

## Task 1: Scaffold — name constant

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java`

- [ ] **Step 1: Add the command name constant**

Find the block of `COMMAND_NAME_*` constants (around lines 71–111). Insert a new line alphabetically near the other "command name" constants, e.g. after `COMMAND_NAME_PUBSUB_SINK`:

```java
String COMMAND_NAME_PII_MASK = "piimask";
```

- [ ] **Step 2: Compile**

Run: `./gradlew :lib:compileJava`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 3: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java
git commit -m "feat(piimask): add piimask command name constant"
```

---

## Task 2: DottedPath

A small immutable type representing a parsed target path.

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/DottedPath.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/piimask/DottedPathTest.java`

- [ ] **Step 1: Write failing tests**

`DottedPathTest.java`:

```java
package io.fleak.zephflow.lib.commands.piimask;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class DottedPathTest {

  @Test
  void parsesSingleSegment() {
    DottedPath p = DottedPath.parse("$.foo");
    assertEquals(List.of("foo"), p.segments());
  }

  @Test
  void parsesNestedPath() {
    DottedPath p = DottedPath.parse("$.user.email");
    assertEquals(List.of("user", "email"), p.segments());
  }

  @Test
  void parsesUnderscoresAndDigits() {
    DottedPath p = DottedPath.parse("$.user_1.field2");
    assertEquals(List.of("user_1", "field2"), p.segments());
  }

  @Test
  void rejectsMissingDollarPrefix() {
    assertThrows(IllegalArgumentException.class, () -> DottedPath.parse("foo.bar"));
  }

  @Test
  void rejectsBracketIndexing() {
    assertThrows(IllegalArgumentException.class, () -> DottedPath.parse("$.foo[0]"));
  }

  @Test
  void rejectsWildcard() {
    assertThrows(IllegalArgumentException.class, () -> DottedPath.parse("$.foo.*"));
  }

  @Test
  void rejectsLeadingDigitSegment() {
    assertThrows(IllegalArgumentException.class, () -> DottedPath.parse("$.0bad"));
  }

  @Test
  void rejectsEmptyPath() {
    assertThrows(IllegalArgumentException.class, () -> DottedPath.parse("$"));
    assertThrows(IllegalArgumentException.class, () -> DottedPath.parse(""));
    assertThrows(IllegalArgumentException.class, () -> DottedPath.parse(null));
  }
}
```

- [ ] **Step 2: Run tests, confirm they fail**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.piimask.DottedPathTest'`
Expected: compile failure (DottedPath doesn't exist).

- [ ] **Step 3: Implement DottedPath**

`DottedPath.java` (copy the license header from `EvalCommand.java`):

```java
package io.fleak.zephflow.lib.commands.piimask;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public record DottedPath(List<String> segments) {

  public static final Pattern PATTERN = Pattern.compile("^\\$(\\.[A-Za-z_][A-Za-z0-9_]*)+$");

  public static DottedPath parse(String raw) {
    if (raw == null || !PATTERN.matcher(raw).matches()) {
      throw new IllegalArgumentException(
          "Invalid PII mask target path: " + raw
              + ". Expected simple dotted form like $.foo.bar");
    }
    String body = raw.substring(2);
    return new DottedPath(List.of(body.split("\\.")));
  }
}
```

- [ ] **Step 4: Run tests, confirm they pass**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.piimask.DottedPathTest'`
Expected: `BUILD SUCCESSFUL`, 8 tests pass.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/DottedPath.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/piimask/DottedPathTest.java
git commit -m "feat(piimask): add DottedPath parser for target paths"
```

---

## Task 3: PiiPathWriter

Reads the value at a parsed path and rewrites string leaves (or string elements inside an array leaf) using a string-rewriter function.

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/PiiPathWriter.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/piimask/PiiPathWriterTest.java`

- [ ] **Step 1: Write failing tests**

`PiiPathWriterTest.java`:

```java
package io.fleak.zephflow.lib.commands.piimask;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PiiPathWriterTest {

  /** A rewriter that uppercases the input — easy to verify mutation. */
  private static final PiiPathWriter.StringRewriter UPPER =
      s -> new PiiPathWriter.RewriteResult(s.toUpperCase(), 1);

  @Test
  void rewritesTopLevelStringLeaf() {
    RecordFleakData rec = record(Map.of("foo", str("hello")));
    int n = PiiPathWriter.rewrite(rec, DottedPath.parse("$.foo"), UPPER);
    assertEquals(1, n);
    assertEquals("HELLO", ((StringPrimitiveFleakData) rec.getPayload().get("foo")).getStringValue());
  }

  @Test
  void rewritesNestedStringLeaf() {
    RecordFleakData rec = record(Map.of("user", record(Map.of("email", str("a@b.com")))));
    int n = PiiPathWriter.rewrite(rec, DottedPath.parse("$.user.email"), UPPER);
    assertEquals(1, n);
    RecordFleakData inner = (RecordFleakData) rec.getPayload().get("user");
    assertEquals("A@B.COM", ((StringPrimitiveFleakData) inner.getPayload().get("email")).getStringValue());
  }

  @Test
  void rewritesEachStringInArrayLeaf() {
    RecordFleakData rec = record(Map.of("recipients",
        new ArrayFleakData(List.of(str("a"), str("b")))));
    int n = PiiPathWriter.rewrite(rec, DottedPath.parse("$.recipients"), UPPER);
    assertEquals(2, n);
    ArrayFleakData arr = (ArrayFleakData) rec.getPayload().get("recipients");
    assertEquals("A", ((StringPrimitiveFleakData) arr.getArrayPayload().get(0)).getStringValue());
    assertEquals("B", ((StringPrimitiveFleakData) arr.getArrayPayload().get(1)).getStringValue());
  }

  @Test
  void skipsNonStringElementsInArray() {
    RecordFleakData rec = record(Map.of("mixed",
        new ArrayFleakData(List.of(str("a"), new NumberPrimitiveFleakData(1.0,
            NumberPrimitiveFleakData.NumberType.LONG), str("c")))));
    int n = PiiPathWriter.rewrite(rec, DottedPath.parse("$.mixed"), UPPER);
    assertEquals(2, n);
    ArrayFleakData arr = (ArrayFleakData) rec.getPayload().get("mixed");
    assertEquals("A", ((StringPrimitiveFleakData) arr.getArrayPayload().get(0)).getStringValue());
    assertInstanceOf(NumberPrimitiveFleakData.class, arr.getArrayPayload().get(1));
    assertEquals("C", ((StringPrimitiveFleakData) arr.getArrayPayload().get(2)).getStringValue());
  }

  @Test
  void skipsMissingTopLevelKey() {
    RecordFleakData rec = record(Map.of("foo", str("hello")));
    int n = PiiPathWriter.rewrite(rec, DottedPath.parse("$.bar"), UPPER);
    assertEquals(0, n);
    assertEquals("hello", ((StringPrimitiveFleakData) rec.getPayload().get("foo")).getStringValue());
  }

  @Test
  void skipsMissingNestedKey() {
    RecordFleakData rec = record(Map.of("user", record(Map.of("name", str("dan")))));
    int n = PiiPathWriter.rewrite(rec, DottedPath.parse("$.user.email"), UPPER);
    assertEquals(0, n);
  }

  @Test
  void skipsWhenIntermediateIsNotRecord() {
    RecordFleakData rec = record(Map.of("user", str("not-a-record")));
    int n = PiiPathWriter.rewrite(rec, DottedPath.parse("$.user.email"), UPPER);
    assertEquals(0, n);
    assertEquals("not-a-record",
        ((StringPrimitiveFleakData) rec.getPayload().get("user")).getStringValue());
  }

  @Test
  void skipsWhenLeafIsNonStringScalar() {
    RecordFleakData rec = record(Map.of("count",
        new NumberPrimitiveFleakData(42.0, NumberPrimitiveFleakData.NumberType.LONG)));
    int n = PiiPathWriter.rewrite(rec, DottedPath.parse("$.count"), UPPER);
    assertEquals(0, n);
  }

  private static StringPrimitiveFleakData str(String s) {
    return new StringPrimitiveFleakData(s);
  }

  private static RecordFleakData record(Map<String, FleakData> payload) {
    return new RecordFleakData(new HashMap<>(payload));
  }
}
```

- [ ] **Step 2: Run tests, confirm they fail**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.piimask.PiiPathWriterTest'`
Expected: compile failure (PiiPathWriter doesn't exist).

- [ ] **Step 3: Implement PiiPathWriter**

`PiiPathWriter.java`:

```java
package io.fleak.zephflow.lib.commands.piimask;

import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import java.util.List;

public final class PiiPathWriter {

  private PiiPathWriter() {}

  @FunctionalInterface
  public interface StringRewriter {
    RewriteResult rewrite(String input);
  }

  public record RewriteResult(String output, int replacementCount) {}

  /**
   * Walks the record by path. If the leaf is a string, rewrites it via
   * {@code rewriter} and replaces in place. If the leaf is an array, rewrites
   * each string element in place; non-string elements are left untouched.
   * Returns the total number of replacements made across all rewritten strings.
   * Returns 0 silently on any of: missing key, intermediate non-record value,
   * non-string/non-array leaf, null record.
   */
  public static int rewrite(RecordFleakData record, DottedPath path, StringRewriter rewriter) {
    if (record == null) {
      return 0;
    }
    List<String> segments = path.segments();
    RecordFleakData cursor = record;
    for (int i = 0; i < segments.size() - 1; i++) {
      FleakData next = cursor.getPayload().get(segments.get(i));
      if (!(next instanceof RecordFleakData nextRecord)) {
        return 0;
      }
      cursor = nextRecord;
    }
    String leafKey = segments.get(segments.size() - 1);
    FleakData leaf = cursor.getPayload().get(leafKey);
    if (leaf instanceof StringPrimitiveFleakData s) {
      RewriteResult r = rewriter.rewrite(s.getStringValue());
      s.setStringValue(r.output());
      return r.replacementCount();
    }
    if (leaf instanceof ArrayFleakData arr) {
      int total = 0;
      for (FleakData element : arr.getArrayPayload()) {
        if (element instanceof StringPrimitiveFleakData es) {
          RewriteResult r = rewriter.rewrite(es.getStringValue());
          es.setStringValue(r.output());
          total += r.replacementCount();
        }
      }
      return total;
    }
    return 0;
  }
}
```

- [ ] **Step 4: Run tests, confirm they pass**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.piimask.PiiPathWriterTest'`
Expected: all 8 tests pass.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/PiiPathWriter.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/piimask/PiiPathWriterTest.java
git commit -m "feat(piimask): add PiiPathWriter for in-place leaf rewrites"
```

---

## Task 4: Built-in detectors

A typed enumeration of the six detectors. Each carries its name, default token, compiled pattern, and an optional post-filter predicate that runs on each match candidate (returns `true` to keep the match, `false` to leave it untouched). Patterns are compiled once and reused.

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/BuiltInDetectors.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/piimask/BuiltInDetectorsTest.java`

- [ ] **Step 1: Write failing tests**

`BuiltInDetectorsTest.java`:

```java
package io.fleak.zephflow.lib.commands.piimask;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class BuiltInDetectorsTest {

  @Test
  void emailMatches() {
    assertTrue(matches(BuiltInDetectors.EMAIL, "contact me at jane.doe+work@example.co"));
    assertFalse(matches(BuiltInDetectors.EMAIL, "no at sign here"));
  }

  @Test
  void phoneMatchesUsFormats() {
    assertTrue(matches(BuiltInDetectors.PHONE, "415-555-1234"));
    assertTrue(matches(BuiltInDetectors.PHONE, "(415) 555-1234"));
    assertTrue(matches(BuiltInDetectors.PHONE, "+1 415 555 1234"));
  }

  @Test
  void phoneMatchesE164() {
    assertTrue(matches(BuiltInDetectors.PHONE, "+442071838750"));
  }

  @Test
  void ssnMatchesDashedAndPlain() {
    assertTrue(matches(BuiltInDetectors.SSN, "123-45-6789"));
    assertTrue(matches(BuiltInDetectors.SSN, "ssn 123456789"));
  }

  @Test
  void ssnRejectsForbiddenAreaNumbers() {
    assertFalse(matches(BuiltInDetectors.SSN, "ssn 000456789"));
    assertFalse(matches(BuiltInDetectors.SSN, "ssn 666456789"));
  }

  @Test
  void creditCardPatternMatchesLongDigitRuns() {
    // Pattern only — Luhn happens in PiiMasker, not here.
    assertTrue(matches(BuiltInDetectors.CREDIT_CARD, "4111-1111-1111-1111"));
    assertTrue(matches(BuiltInDetectors.CREDIT_CARD, "4111111111111111"));
    assertFalse(matches(BuiltInDetectors.CREDIT_CARD, "411 22 33"));
  }

  @Test
  void ipv4MatchesShape() {
    // Pattern only — octet validation happens in PiiMasker.
    assertTrue(matches(BuiltInDetectors.IPV4, "192.168.0.1"));
    assertTrue(matches(BuiltInDetectors.IPV4, "999.1.1.1"));
    assertFalse(matches(BuiltInDetectors.IPV4, "1.2.3"));
  }

  @Test
  void ipv6Matches() {
    assertTrue(matches(BuiltInDetectors.IPV6, "fe80::1ff:fe23:4567:890a"));
    assertTrue(matches(BuiltInDetectors.IPV6, "2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
    assertFalse(matches(BuiltInDetectors.IPV6, "12:34"));
  }

  @Test
  void defaultTokensMatchSpec() {
    assertEquals("[EMAIL]", BuiltInDetectors.EMAIL.defaultToken());
    assertEquals("[PHONE]", BuiltInDetectors.PHONE.defaultToken());
    assertEquals("[SSN]", BuiltInDetectors.SSN.defaultToken());
    assertEquals("[CC]", BuiltInDetectors.CREDIT_CARD.defaultToken());
    assertEquals("[IPV4]", BuiltInDetectors.IPV4.defaultToken());
    assertEquals("[IPV6]", BuiltInDetectors.IPV6.defaultToken());
  }

  private static boolean matches(BuiltInDetectors d, String input) {
    return d.pattern().matcher(input).find();
  }
}
```

- [ ] **Step 2: Run tests, confirm they fail**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.piimask.BuiltInDetectorsTest'`
Expected: compile failure.

- [ ] **Step 3: Implement BuiltInDetectors**

`BuiltInDetectors.java`:

```java
package io.fleak.zephflow.lib.commands.piimask;

import java.util.regex.Pattern;

public enum BuiltInDetectors {

  EMAIL("email", "[EMAIL]",
      "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),

  PHONE("phone", "[PHONE]",
      // E.164 international first (so a leading + is consumed as a whole), then US.
      // Order matters: regex alternation is left-to-right; without the E.164-first
      // ordering, the US alternative would match the trailing digits of an E.164
      // number and leave a stray "+" in the output.
      "\\+[2-9]\\d{1,14}"
          + "|(?:\\+?1[-.\\s]?)?\\(?\\d{3}\\)?[-.\\s]?\\d{3}[-.\\s]?\\d{4}"),

  SSN("ssn", "[SSN]",
      // dashed OR no-dash with area/group/serial validity (no 000/666 area, no 00 group, no 0000 serial)
      "\\b\\d{3}-\\d{2}-\\d{4}\\b"
          + "|\\b(?!000|666)[0-8]\\d{2}(?!00)\\d{2}(?!0000)\\d{4}\\b"),

  CREDIT_CARD("creditCard", "[CC]",
      "\\b(?:\\d[ -]*?){13,19}\\b"),

  IPV4("ipv4", "[IPV4]",
      "\\b(?:\\d{1,3}\\.){3}\\d{1,3}\\b"),

  IPV6("ipv6", "[IPV6]",
      // catches full and ::-compressed forms; requires at least 2 colons to avoid `aa:bb`
      "(?:[A-Fa-f0-9]{1,4}:){2,7}[A-Fa-f0-9]{1,4}"
          + "|(?:[A-Fa-f0-9]{1,4}:){1,7}:");

  private final String configKey;
  private final String defaultToken;
  private final Pattern pattern;

  BuiltInDetectors(String configKey, String defaultToken, String regex) {
    this.configKey = configKey;
    this.defaultToken = defaultToken;
    this.pattern = Pattern.compile(regex);
  }

  public String configKey() {
    return configKey;
  }

  public String defaultToken() {
    return defaultToken;
  }

  public Pattern pattern() {
    return pattern;
  }
}
```

- [ ] **Step 4: Run tests, confirm they pass**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.piimask.BuiltInDetectorsTest'`
Expected: all 9 tests pass.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/BuiltInDetectors.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/piimask/BuiltInDetectorsTest.java
git commit -m "feat(piimask): add built-in PII detector patterns"
```

---

## Task 5: PiiMasker

Given a list of active detector specs (each with a `Pattern`, replacement, and optional post-filter) plus a string, runs every detector in declared order and returns `(maskedString, replacementCount)`. Handles Luhn for credit cards and per-octet validation for IPv4 via post-filters.

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/PiiMasker.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskerTest.java`

- [ ] **Step 1: Write failing tests**

`PiiMaskerTest.java`:

```java
package io.fleak.zephflow.lib.commands.piimask;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class PiiMaskerTest {

  @Test
  void replacesEmailWithDefaultToken() {
    PiiMasker.Spec email = PiiMasker.builtIn(BuiltInDetectors.EMAIL, null);
    PiiPathWriter.RewriteResult r = PiiMasker.mask("write me at a@b.com please", List.of(email));
    assertEquals("write me at [EMAIL] please", r.output());
    assertEquals(1, r.replacementCount());
  }

  @Test
  void replacesEmailWithCustomReplacement() {
    PiiMasker.Spec email = PiiMasker.builtIn(BuiltInDetectors.EMAIL, "<redacted-email>");
    PiiPathWriter.RewriteResult r = PiiMasker.mask("a@b.com", List.of(email));
    assertEquals("<redacted-email>", r.output());
    assertEquals(1, r.replacementCount());
  }

  @Test
  void phoneE164ConsumesEntireNumber() {
    PiiMasker.Spec phone = PiiMasker.builtIn(BuiltInDetectors.PHONE, null);
    // Whole +442071838750 must be consumed — no stray '+' left behind.
    PiiPathWriter.RewriteResult r = PiiMasker.mask("call +442071838750 now", List.of(phone));
    assertEquals("call [PHONE] now", r.output());
    assertEquals(1, r.replacementCount());
  }

  @Test
  void phoneUsFormatMatches() {
    PiiMasker.Spec phone = PiiMasker.builtIn(BuiltInDetectors.PHONE, null);
    PiiPathWriter.RewriteResult r = PiiMasker.mask("ring 415-555-1234", List.of(phone));
    assertEquals("ring [PHONE]", r.output());
    assertEquals(1, r.replacementCount());
  }

  @Test
  void replacesMultipleMatches() {
    PiiMasker.Spec email = PiiMasker.builtIn(BuiltInDetectors.EMAIL, null);
    PiiPathWriter.RewriteResult r = PiiMasker.mask("a@b.com and c@d.com", List.of(email));
    assertEquals("[EMAIL] and [EMAIL]", r.output());
    assertEquals(2, r.replacementCount());
  }

  @Test
  void luhnKeepsValidCardAndLeavesInvalidIntact() {
    PiiMasker.Spec cc = PiiMasker.builtIn(BuiltInDetectors.CREDIT_CARD, null);
    PiiPathWriter.RewriteResult r =
        PiiMasker.mask("good 4111-1111-1111-1111 bad 1234-5678-9012-3456", List.of(cc));
    assertEquals("good [CC] bad 1234-5678-9012-3456", r.output());
    assertEquals(1, r.replacementCount());
  }

  @Test
  void ipv4RejectsOutOfRangeOctets() {
    PiiMasker.Spec ip = PiiMasker.builtIn(BuiltInDetectors.IPV4, null);
    PiiPathWriter.RewriteResult r =
        PiiMasker.mask("ok 192.168.0.1 bad 999.1.1.1", List.of(ip));
    assertEquals("ok [IPV4] bad 999.1.1.1", r.output());
    assertEquals(1, r.replacementCount());
  }

  @Test
  void detectorsApplyInDeclaredOrder() {
    // custom first, then email — custom replacement is opaque to the email detector
    PiiMasker.Spec custom = PiiMasker.custom("override",
        Pattern.compile("a@b\\.com"), "<custom>");
    PiiMasker.Spec email = PiiMasker.builtIn(BuiltInDetectors.EMAIL, null);
    PiiPathWriter.RewriteResult r =
        PiiMasker.mask("a@b.com and c@d.com", List.of(custom, email));
    assertEquals("<custom> and [EMAIL]", r.output());
    assertEquals(2, r.replacementCount());
  }

  @Test
  void noMatchesYieldsZeroAndUnchangedInput() {
    PiiMasker.Spec email = PiiMasker.builtIn(BuiltInDetectors.EMAIL, null);
    PiiPathWriter.RewriteResult r = PiiMasker.mask("no pii here", List.of(email));
    assertEquals("no pii here", r.output());
    assertEquals(0, r.replacementCount());
  }

  @Test
  void emptyStringYieldsZero() {
    PiiMasker.Spec email = PiiMasker.builtIn(BuiltInDetectors.EMAIL, null);
    PiiPathWriter.RewriteResult r = PiiMasker.mask("", List.of(email));
    assertEquals("", r.output());
    assertEquals(0, r.replacementCount());
  }

  @Test
  void luhnHelperRejectsAllZeros() {
    assertFalse(PiiMasker.luhnValid("0000000000000000"));
  }

  @Test
  void luhnHelperAcceptsKnownValidCard() {
    assertTrue(PiiMasker.luhnValid("4111111111111111"));
  }
}
```

- [ ] **Step 2: Run tests, confirm they fail**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.piimask.PiiMaskerTest'`
Expected: compile failure.

- [ ] **Step 3: Implement PiiMasker**

`PiiMasker.java`:

```java
package io.fleak.zephflow.lib.commands.piimask;

import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PiiMasker {

  private PiiMasker() {}

  public record Spec(Pattern pattern, String replacement, Predicate<String> postFilter) {
    public boolean accept(String matchText) {
      return postFilter == null || postFilter.test(matchText);
    }
  }

  public static Spec builtIn(BuiltInDetectors d, String customReplacement) {
    String replacement = customReplacement != null ? customReplacement : d.defaultToken();
    Predicate<String> postFilter = switch (d) {
      case CREDIT_CARD -> PiiMasker::luhnValid;
      case IPV4 -> PiiMasker::ipv4OctetsValid;
      default -> null;
    };
    return new Spec(d.pattern(), replacement, postFilter);
  }

  public static Spec custom(String name, Pattern pattern, String replacement) {
    return new Spec(pattern, replacement, null);
  }

  public static PiiPathWriter.RewriteResult mask(String input, List<Spec> specs) {
    String current = input;
    int total = 0;
    for (Spec spec : specs) {
      Matcher matcher = spec.pattern().matcher(current);
      StringBuilder sb = new StringBuilder();
      int count = 0;
      while (matcher.find()) {
        if (spec.accept(matcher.group())) {
          matcher.appendReplacement(sb, Matcher.quoteReplacement(spec.replacement()));
          count++;
        } else {
          matcher.appendReplacement(sb, Matcher.quoteReplacement(matcher.group()));
        }
      }
      matcher.appendTail(sb);
      current = sb.toString();
      total += count;
    }
    return new PiiPathWriter.RewriteResult(current, total);
  }

  public static boolean luhnValid(String matchText) {
    String digits = matchText.replaceAll("[\\s-]", "");
    if (digits.length() < 13 || digits.length() > 19) {
      return false;
    }
    int sum = 0;
    boolean alt = false;
    for (int i = digits.length() - 1; i >= 0; i--) {
      char c = digits.charAt(i);
      if (c < '0' || c > '9') {
        return false;
      }
      int n = c - '0';
      if (alt) {
        n *= 2;
        if (n > 9) n -= 9;
      }
      sum += n;
      alt = !alt;
    }
    return sum > 0 && sum % 10 == 0;
  }

  public static boolean ipv4OctetsValid(String matchText) {
    String[] parts = matchText.split("\\.");
    if (parts.length != 4) {
      return false;
    }
    for (String part : parts) {
      try {
        int v = Integer.parseInt(part);
        if (v < 0 || v > 255) return false;
      } catch (NumberFormatException e) {
        return false;
      }
    }
    return true;
  }
}
```

- [ ] **Step 4: Run tests, confirm they pass**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.piimask.PiiMaskerTest'`
Expected: all 12 tests pass.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/PiiMasker.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskerTest.java
git commit -m "feat(piimask): add PiiMasker with Luhn and IPv4 post-filters"
```

---

## Task 6: DTO records

No tests — these are pure data carriers. Just define them.

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskCommandDto.java`

- [ ] **Step 1: Create the DTO**

```java
package io.fleak.zephflow.lib.commands.piimask;

import io.fleak.zephflow.api.CommandConfig;
import java.util.List;

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

  // null replacement -> fall back to the built-in default token
  record DetectorConfig(String replacement) {}

  record CustomPattern(String name, String pattern, String replacement) {}
}
```

- [ ] **Step 2: Compile**

Run: `./gradlew :lib:compileJava`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 3: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskCommandDto.java
git commit -m "feat(piimask): add command DTO records"
```

---

## Task 7: ConfigParser

Parses a raw `Map<String,Object>` into `PiiMaskCommandDto.Config`. Rule: a built-in detector is active iff the map under `detectors` contains its key with a non-null value. Custom patterns require all three fields non-null/non-blank to even reach the validator; the parser tolerates missing customPatterns/detectors entirely.

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskConfigParser.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskConfigParserTest.java`

- [ ] **Step 1: Write failing tests**

`PiiMaskConfigParserTest.java`:

```java
package io.fleak.zephflow.lib.commands.piimask;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Config;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.CustomPattern;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PiiMaskConfigParserTest {

  private final PiiMaskConfigParser parser = new PiiMaskConfigParser();

  @Test
  void parsesTargetsOnly() {
    Config c = (Config) parser.parseConfig(Map.of("targets", List.of("$.foo")));
    assertEquals(List.of("$.foo"), c.targets());
    assertNull(c.detectors());
    assertNull(c.customPatterns());
  }

  @Test
  void parsesActiveBuiltInWithDefaultReplacement() {
    Config c = (Config) parser.parseConfig(Map.of(
        "targets", List.of("$.x"),
        "detectors", Map.of("email", Map.of())));
    assertNotNull(c.detectors());
    assertNotNull(c.detectors().email());
    assertNull(c.detectors().email().replacement());
    assertNull(c.detectors().phone());
  }

  @Test
  void parsesActiveBuiltInWithCustomReplacement() {
    Config c = (Config) parser.parseConfig(Map.of(
        "targets", List.of("$.x"),
        "detectors", Map.of("email", Map.of("replacement", "<E>"))));
    assertEquals("<E>", c.detectors().email().replacement());
  }

  @Test
  void detectorPresentWithNullMapDisablesIt() {
    java.util.Map<String, Object> root = new java.util.HashMap<>();
    root.put("targets", List.of("$.x"));
    java.util.Map<String, Object> det = new java.util.HashMap<>();
    det.put("email", null);
    det.put("phone", Map.of());
    root.put("detectors", det);
    Config c = (Config) parser.parseConfig(root);
    assertNull(c.detectors().email());
    assertNotNull(c.detectors().phone());
  }

  @Test
  void parsesCustomPatterns() {
    Config c = (Config) parser.parseConfig(Map.of(
        "targets", List.of("$.x"),
        "customPatterns", List.of(
            Map.of("name", "id", "pattern", "INT-\\d+", "replacement", "<ID>"))));
    assertEquals(1, c.customPatterns().size());
    CustomPattern cp = c.customPatterns().get(0);
    assertEquals("id", cp.name());
    assertEquals("INT-\\d+", cp.pattern());
    assertEquals("<ID>", cp.replacement());
  }

  @Test
  void targetsMustBePresent() {
    assertThrows(IllegalArgumentException.class,
        () -> parser.parseConfig(Map.of("detectors", Map.of("email", Map.of()))));
  }
}
```

- [ ] **Step 2: Run tests, confirm they fail**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.piimask.PiiMaskConfigParserTest'`
Expected: compile failure.

- [ ] **Step 3: Implement PiiMaskConfigParser**

```java
package io.fleak.zephflow.lib.commands.piimask;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigParser;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Config;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.CustomPattern;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.DetectorConfig;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Detectors;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PiiMaskConfigParser implements ConfigParser {

  @Override
  @SuppressWarnings("unchecked")
  public CommandConfig parseConfig(Map<String, Object> config) {
    Object rawTargets = config.get("targets");
    Preconditions.checkArgument(
        rawTargets instanceof List<?>,
        "piimask command requires 'targets' to be a list of paths");
    List<String> targets = new ArrayList<>();
    for (Object t : (List<Object>) rawTargets) {
      targets.add(String.valueOf(t));
    }

    Detectors detectors = null;
    Object rawDetectors = config.get("detectors");
    if (rawDetectors instanceof Map<?, ?> dm) {
      detectors = new Detectors(
          parseDetector(dm, "email"),
          parseDetector(dm, "phone"),
          parseDetector(dm, "ssn"),
          parseDetector(dm, "creditCard"),
          parseDetector(dm, "ipv4"),
          parseDetector(dm, "ipv6"));
    }

    List<CustomPattern> customPatterns = null;
    Object rawCustom = config.get("customPatterns");
    if (rawCustom instanceof List<?> cl) {
      customPatterns = new ArrayList<>();
      for (Object e : cl) {
        if (e instanceof Map<?, ?> em) {
          customPatterns.add(new CustomPattern(
              stringOrNull(em.get("name")),
              stringOrNull(em.get("pattern")),
              stringOrNull(em.get("replacement"))));
        }
      }
    }

    return new Config(targets, detectors, customPatterns);
  }

  private static DetectorConfig parseDetector(Map<?, ?> detectors, String key) {
    if (!detectors.containsKey(key)) return null;
    Object v = detectors.get(key);
    if (!(v instanceof Map<?, ?> m)) return null;
    return new DetectorConfig(stringOrNull(m.get("replacement")));
  }

  private static String stringOrNull(Object v) {
    return v == null ? null : String.valueOf(v);
  }
}
```

- [ ] **Step 4: Run tests, confirm they pass**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.piimask.PiiMaskConfigParserTest'`
Expected: all 6 tests pass.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskConfigParser.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskConfigParserTest.java
git commit -m "feat(piimask): add config parser"
```

---

## Task 8: ConfigValidator

Validates a parsed `Config` at DAG compile time. Compiles every regex and parses every target path; failure here is a setup error, not a runtime error.

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskConfigValidator.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskConfigValidatorTest.java`

- [ ] **Step 1: Write failing tests**

`PiiMaskConfigValidatorTest.java`:

```java
package io.fleak.zephflow.lib.commands.piimask;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Config;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.CustomPattern;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.DetectorConfig;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Detectors;
import java.util.List;
import org.junit.jupiter.api.Test;

class PiiMaskConfigValidatorTest {

  private final PiiMaskConfigValidator v = new PiiMaskConfigValidator();

  private static Detectors emailOnly() {
    return new Detectors(new DetectorConfig(null), null, null, null, null, null);
  }

  @Test
  void acceptsMinimalValidConfig() {
    Config c = new Config(List.of("$.foo"), emailOnly(), null);
    assertDoesNotThrow(() -> v.validateConfig(c, "n1", null));
  }

  @Test
  void rejectsEmptyTargets() {
    Config c = new Config(List.of(), emailOnly(), null);
    assertThrows(IllegalArgumentException.class, () -> v.validateConfig(c, "n1", null));
  }

  @Test
  void rejectsMalformedTarget() {
    Config c = new Config(List.of("$.foo[0]"), emailOnly(), null);
    assertThrows(IllegalArgumentException.class, () -> v.validateConfig(c, "n1", null));
  }

  @Test
  void rejectsConfigWithNoActiveDetectorsAndNoCustom() {
    Config c = new Config(List.of("$.foo"), null, null);
    assertThrows(IllegalArgumentException.class, () -> v.validateConfig(c, "n1", null));
  }

  @Test
  void acceptsConfigWithOnlyCustomPatterns() {
    Config c = new Config(List.of("$.foo"), null,
        List.of(new CustomPattern("id", "X-\\d+", "<ID>")));
    assertDoesNotThrow(() -> v.validateConfig(c, "n1", null));
  }

  @Test
  void rejectsBlankCustomName() {
    Config c = new Config(List.of("$.foo"), null,
        List.of(new CustomPattern("  ", "X-\\d+", "<ID>")));
    assertThrows(IllegalArgumentException.class, () -> v.validateConfig(c, "n1", null));
  }

  @Test
  void rejectsBlankCustomPattern() {
    Config c = new Config(List.of("$.foo"), null,
        List.of(new CustomPattern("id", "", "<ID>")));
    assertThrows(IllegalArgumentException.class, () -> v.validateConfig(c, "n1", null));
  }

  @Test
  void rejectsBlankCustomReplacement() {
    Config c = new Config(List.of("$.foo"), null,
        List.of(new CustomPattern("id", "X-\\d+", null)));
    assertThrows(IllegalArgumentException.class, () -> v.validateConfig(c, "n1", null));
  }

  @Test
  void rejectsInvalidCustomRegex() {
    Config c = new Config(List.of("$.foo"), null,
        List.of(new CustomPattern("id", "(", "<ID>")));
    assertThrows(IllegalArgumentException.class, () -> v.validateConfig(c, "n1", null));
  }

  @Test
  void rejectsDuplicateCustomNames() {
    Config c = new Config(List.of("$.foo"), null,
        List.of(
            new CustomPattern("dup", "A", "<A>"),
            new CustomPattern("dup", "B", "<B>")));
    assertThrows(IllegalArgumentException.class, () -> v.validateConfig(c, "n1", null));
  }
}
```

- [ ] **Step 2: Run tests, confirm they fail**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.piimask.PiiMaskConfigValidatorTest'`
Expected: compile failure.

- [ ] **Step 3: Implement PiiMaskConfigValidator**

```java
package io.fleak.zephflow.lib.commands.piimask;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Config;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.CustomPattern;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Detectors;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class PiiMaskConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    Config c = (Config) commandConfig;

    Preconditions.checkArgument(
        c.targets() != null && !c.targets().isEmpty(),
        "piimask: 'targets' must contain at least one path");

    for (String t : c.targets()) {
      try {
        DottedPath.parse(t);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("piimask: " + e.getMessage(), e);
      }
    }

    boolean anyBuiltInActive = hasActiveBuiltIn(c.detectors());
    boolean anyCustom = c.customPatterns() != null && !c.customPatterns().isEmpty();
    Preconditions.checkArgument(
        anyBuiltInActive || anyCustom,
        "piimask: at least one built-in detector or one customPatterns entry must be configured");

    if (anyCustom) {
      Set<String> seenNames = new HashSet<>();
      for (CustomPattern cp : c.customPatterns()) {
        Preconditions.checkArgument(
            !Strings.isNullOrEmpty(cp.name()) && !cp.name().isBlank(),
            "piimask: customPatterns entries must have a non-blank 'name'");
        Preconditions.checkArgument(
            !Strings.isNullOrEmpty(cp.pattern()) && !cp.pattern().isBlank(),
            "piimask: customPatterns entry '%s' must have a non-blank 'pattern'", cp.name());
        Preconditions.checkArgument(
            !Strings.isNullOrEmpty(cp.replacement()) && !cp.replacement().isBlank(),
            "piimask: customPatterns entry '%s' must have a non-blank 'replacement'", cp.name());
        Preconditions.checkArgument(
            seenNames.add(cp.name()),
            "piimask: duplicate customPatterns name '%s'", cp.name());
        try {
          Pattern.compile(cp.pattern());
        } catch (PatternSyntaxException e) {
          throw new IllegalArgumentException(
              "piimask: customPatterns entry '" + cp.name() + "' has invalid regex: "
                  + e.getMessage(), e);
        }
      }
    }
  }

  private static boolean hasActiveBuiltIn(Detectors d) {
    if (d == null) return false;
    return d.email() != null
        || d.phone() != null
        || d.ssn() != null
        || d.creditCard() != null
        || d.ipv4() != null
        || d.ipv6() != null;
  }
}
```

- [ ] **Step 4: Run tests, confirm they pass**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.piimask.PiiMaskConfigValidatorTest'`
Expected: all 10 tests pass.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskConfigValidator.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskConfigValidatorTest.java
git commit -m "feat(piimask): add config validator"
```

---

## Task 9: PiiMaskExecutionContext

Holds the compiled, ready-to-use form of the config: parsed `DottedPath`s, an ordered list of `PiiMasker.Spec`s, and the `pii_match_count` counter.

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskExecutionContext.java`

- [ ] **Step 1: Implement the execution context**

```java
package io.fleak.zephflow.lib.commands.piimask;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.lib.commands.DefaultExecutionContext;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;

@EqualsAndHashCode(callSuper = true)
@Value
public class PiiMaskExecutionContext extends DefaultExecutionContext {

  List<DottedPath> targets;
  List<PiiMasker.Spec> specs;
  FleakCounter piiMatchCounter;

  public PiiMaskExecutionContext(
      FleakCounter inputMessageCounter,
      FleakCounter outputMessageCounter,
      FleakCounter errorCounter,
      List<DottedPath> targets,
      List<PiiMasker.Spec> specs,
      FleakCounter piiMatchCounter) {
    super(inputMessageCounter, outputMessageCounter, errorCounter);
    this.targets = targets;
    this.specs = specs;
    this.piiMatchCounter = piiMatchCounter;
  }
}
```

- [ ] **Step 2: Compile**

Run: `./gradlew :lib:compileJava`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 3: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskExecutionContext.java
git commit -m "feat(piimask): add execution context"
```

---

## Task 10: PiiMaskCommand + Factory + Registry

End-to-end implementation. After this task the node is registered and usable from a DAG definition.

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskCommand.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskCommandFactory.java`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskCommandTest.java`

- [ ] **Step 1: Write failing end-to-end tests**

`PiiMaskCommandTest.java`:

```java
package io.fleak.zephflow.lib.commands.piimask;

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static io.fleak.zephflow.lib.utils.JsonUtils.loadFleakDataFromJsonString;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.ScalarCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PiiMaskCommandTest {

  @Test
  void masksEmailWithDefaultTokenInPlace() {
    RecordFleakData input = json("""
        { "message": "ping a@b.com please" }
        """);

    ScalarCommand.ProcessResult result = run(
        Map.of(
            "targets", List.of("$.message"),
            "detectors", Map.of("email", Map.of())),
        input);

    assertTrue(result.getFailureEvents().isEmpty());
    assertEquals(1, result.getOutput().size());
    assertEquals(
        Map.of("message", "ping [EMAIL] please"),
        JsonUtils.OBJECT_MAPPER.convertValue(result.getOutput().get(0).unwrap(), Map.class));
  }

  @Test
  void disabledDetectorIsNotApplied() {
    RecordFleakData input = json("""
        { "message": "+1 415 555 1234 and a@b.com" }
        """);
    ScalarCommand.ProcessResult result = run(
        Map.of(
            "targets", List.of("$.message"),
            "detectors", Map.of("email", Map.of())),  // phone NOT enabled
        input);
    String masked = (String) ((Map<?, ?>) result.getOutput().get(0).unwrap()).get("message");
    assertTrue(masked.contains("+1 415 555 1234"));
    assertTrue(masked.contains("[EMAIL]"));
  }

  @Test
  void customReplacementOverridesDefault() {
    RecordFleakData input = json("""
        { "message": "a@b.com" }
        """);
    ScalarCommand.ProcessResult result = run(
        Map.of(
            "targets", List.of("$.message"),
            "detectors", Map.of("email", Map.of("replacement", "<E>"))),
        input);
    assertEquals(
        Map.of("message", "<E>"),
        JsonUtils.OBJECT_MAPPER.convertValue(result.getOutput().get(0).unwrap(), Map.class));
  }

  @Test
  void customPatternIsApplied() {
    RecordFleakData input = json("""
        { "message": "ref INT-12345678 in the system" }
        """);
    ScalarCommand.ProcessResult result = run(
        Map.of(
            "targets", List.of("$.message"),
            "customPatterns", List.of(
                Map.of("name", "internalId",
                       "pattern", "INT-\\d{8}",
                       "replacement", "<INT>"))),
        input);
    assertEquals(
        Map.of("message", "ref <INT> in the system"),
        JsonUtils.OBJECT_MAPPER.convertValue(result.getOutput().get(0).unwrap(), Map.class));
  }

  @Test
  void masksAcrossMultipleTargets() {
    RecordFleakData input = json("""
        { "user": { "email": "a@b.com" }, "note": "ssn 123-45-6789 here" }
        """);
    ScalarCommand.ProcessResult result = run(
        Map.of(
            "targets", List.of("$.user.email", "$.note"),
            "detectors", Map.of(
                "email", Map.of(),
                "ssn", Map.of())),
        input);
    Map<?, ?> out = (Map<?, ?>) result.getOutput().get(0).unwrap();
    assertEquals("[EMAIL]", ((Map<?, ?>) out.get("user")).get("email"));
    assertEquals("ssn [SSN] here", out.get("note"));
  }

  @Test
  void missingTargetLeavesRecordUntouched() {
    RecordFleakData input = json("""
        { "message": "a@b.com" }
        """);
    ScalarCommand.ProcessResult result = run(
        Map.of(
            "targets", List.of("$.nope"),
            "detectors", Map.of("email", Map.of())),
        input);
    assertEquals(
        Map.of("message", "a@b.com"),
        JsonUtils.OBJECT_MAPPER.convertValue(result.getOutput().get(0).unwrap(), Map.class));
  }

  @Test
  void nonStringScalarTargetSkipped() {
    RecordFleakData input = json("""
        { "count": 42 }
        """);
    ScalarCommand.ProcessResult result = run(
        Map.of(
            "targets", List.of("$.count"),
            "detectors", Map.of("email", Map.of())),
        input);
    assertEquals(
        Map.of("count", 42L),
        JsonUtils.OBJECT_MAPPER.convertValue(result.getOutput().get(0).unwrap(), Map.class));
  }

  @Test
  void arrayOfStringsHasEachElementMasked() {
    RecordFleakData input = json("""
        { "recipients": ["a@b.com", "c@d.com", "no-pii"] }
        """);
    ScalarCommand.ProcessResult result = run(
        Map.of(
            "targets", List.of("$.recipients"),
            "detectors", Map.of("email", Map.of())),
        input);
    Map<?, ?> out = (Map<?, ?>) result.getOutput().get(0).unwrap();
    assertEquals(List.of("[EMAIL]", "[EMAIL]", "no-pii"), out.get("recipients"));
  }

  @Test
  void mixedArrayMasksOnlyStrings() {
    RecordFleakData input = json("""
        { "mixed": ["a@b.com", 7, "c@d.com"] }
        """);
    ScalarCommand.ProcessResult result = run(
        Map.of(
            "targets", List.of("$.mixed"),
            "detectors", Map.of("email", Map.of())),
        input);
    Map<?, ?> out = (Map<?, ?>) result.getOutput().get(0).unwrap();
    assertEquals(List.of("[EMAIL]", 7L, "[EMAIL]"), out.get("mixed"));
  }

  @Test
  void creditCardLuhnValidReplacedInvalidLeftIntact() {
    RecordFleakData input = json("""
        { "msg": "good 4111-1111-1111-1111 bad 1234-5678-9012-3456" }
        """);
    ScalarCommand.ProcessResult result = run(
        Map.of(
            "targets", List.of("$.msg"),
            "detectors", Map.of("creditCard", Map.of())),
        input);
    Map<?, ?> out = (Map<?, ?>) result.getOutput().get(0).unwrap();
    assertEquals("good [CC] bad 1234-5678-9012-3456", out.get("msg"));
  }

  @Test
  void ipv4OctetValidationRejectsOutOfRange() {
    RecordFleakData input = json("""
        { "msg": "ok 192.168.0.1 bad 999.1.1.1" }
        """);
    ScalarCommand.ProcessResult result = run(
        Map.of(
            "targets", List.of("$.msg"),
            "detectors", Map.of("ipv4", Map.of())),
        input);
    Map<?, ?> out = (Map<?, ?>) result.getOutput().get(0).unwrap();
    assertEquals("ok [IPV4] bad 999.1.1.1", out.get("msg"));
  }

  // --- helpers ---

  private static RecordFleakData json(String s) {
    return (RecordFleakData) loadFleakDataFromJsonString(s);
  }

  private static ScalarCommand.ProcessResult run(
      Map<String, Object> rawConfig, RecordFleakData input) {
    PiiMaskCommand cmd =
        (PiiMaskCommand) new PiiMaskCommandFactory().createCommand("n1", JOB_CONTEXT);
    cmd.parseAndValidateArg(rawConfig);
    cmd.initialize(new MetricClientProvider.NoopMetricClientProvider());
    return cmd.process(List.of(input), "tester", cmd.getExecutionContext());
  }
}
```

- [ ] **Step 2: Run tests, confirm they fail**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandTest'`
Expected: compile failure (no command/factory yet).

- [ ] **Step 3: Implement PiiMaskCommand**

```java
package io.fleak.zephflow.lib.commands.piimask;

import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_PII_MASK;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_ERROR_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_OUTPUT_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.basicCommandMetricTags;
import static io.fleak.zephflow.lib.utils.MiscUtils.getCallingUserTagAndEventTags;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigParser;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.ExecutionContext;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.ScalarCommand;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Config;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.CustomPattern;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Detectors;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class PiiMaskCommand extends ScalarCommand {

  public static final String METRIC_NAME_PII_MATCH_COUNT = "pii_match_count";

  protected PiiMaskCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator) {
    super(nodeId, jobContext, configParser, configValidator);
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    Map<String, String> metricTags =
        basicCommandMetricTags(jobContext.getMetricTags(), commandName(), nodeId);
    FleakCounter input = metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_COUNT, metricTags);
    FleakCounter output = metricClientProvider.counter(METRIC_NAME_OUTPUT_EVENT_COUNT, metricTags);
    FleakCounter error = metricClientProvider.counter(METRIC_NAME_ERROR_EVENT_COUNT, metricTags);
    FleakCounter piiMatch =
        metricClientProvider.counter(METRIC_NAME_PII_MATCH_COUNT, metricTags);

    Config config = (Config) commandConfig;

    List<DottedPath> paths = new ArrayList<>();
    for (String t : config.targets()) {
      paths.add(DottedPath.parse(t));
    }

    List<PiiMasker.Spec> specs = new ArrayList<>();
    if (config.customPatterns() != null) {
      for (CustomPattern cp : config.customPatterns()) {
        specs.add(PiiMasker.custom(cp.name(), Pattern.compile(cp.pattern()), cp.replacement()));
      }
    }
    Detectors d = config.detectors();
    if (d != null) {
      if (d.email() != null) specs.add(PiiMasker.builtIn(BuiltInDetectors.EMAIL, d.email().replacement()));
      if (d.phone() != null) specs.add(PiiMasker.builtIn(BuiltInDetectors.PHONE, d.phone().replacement()));
      if (d.ssn() != null) specs.add(PiiMasker.builtIn(BuiltInDetectors.SSN, d.ssn().replacement()));
      if (d.creditCard() != null) specs.add(PiiMasker.builtIn(BuiltInDetectors.CREDIT_CARD, d.creditCard().replacement()));
      if (d.ipv4() != null) specs.add(PiiMasker.builtIn(BuiltInDetectors.IPV4, d.ipv4().replacement()));
      if (d.ipv6() != null) specs.add(PiiMasker.builtIn(BuiltInDetectors.IPV6, d.ipv6().replacement()));
    }

    return new PiiMaskExecutionContext(input, output, error, paths, specs, piiMatch);
  }

  @Override
  protected List<RecordFleakData> processOneEvent(
      RecordFleakData event, String callingUser, ExecutionContext context) {
    PiiMaskExecutionContext ctx = (PiiMaskExecutionContext) context;
    Map<String, String> tags = getCallingUserTagAndEventTags(callingUser, event);
    ctx.getInputMessageCounter().increase(tags);

    int totalReplacements = 0;
    for (DottedPath path : ctx.getTargets()) {
      totalReplacements += PiiPathWriter.rewrite(
          event, path, s -> PiiMasker.mask(s, ctx.getSpecs()));
    }
    if (totalReplacements > 0) {
      ctx.getPiiMatchCounter().increase(totalReplacements, tags);
    }
    ctx.getOutputMessageCounter().increase(tags);
    return List.of(event);
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_PII_MASK;
  }
}
```

- [ ] **Step 4: Implement PiiMaskCommandFactory**

```java
package io.fleak.zephflow.lib.commands.piimask;

import io.fleak.zephflow.api.CommandFactory;
import io.fleak.zephflow.api.CommandType;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.OperatorCommand;

public class PiiMaskCommandFactory extends CommandFactory {
  @Override
  public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
    return new PiiMaskCommand(
        nodeId, jobContext, new PiiMaskConfigParser(), new PiiMaskConfigValidator());
  }

  @Override
  public CommandType commandType() {
    return CommandType.INTERMEDIATE_COMMAND;
  }
}
```

- [ ] **Step 5: Register in OperatorCommandRegistry**

In `OperatorCommandRegistry.java`:

1. Add the import near the other command-factory imports (alphabetical):
   ```java
   import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandFactory;
   ```

2. Add a registration line at the end of the `OPERATOR_COMMANDS` builder, before `.build()`:
   ```java
   .put(COMMAND_NAME_PII_MASK, new PiiMaskCommandFactory())
   ```

Verify by reading the file that the constant is statically imported (the existing `.put` lines use unqualified `COMMAND_NAME_*` names because of `import static ... MiscUtils.*;`).

- [ ] **Step 6: Run command tests, confirm they pass**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandTest'`
Expected: all 11 tests pass.

(The `FleakCounter` interface in `api/` exposes both `increase(Map<String,String>)` and `increase(long n, Map<String,String>)`, so the `increase(totalReplacements, tags)` call above is correct — `int` auto-widens to `long`.)

- [ ] **Step 7: Run full lib test suite**

Run: `./gradlew :lib:test`
Expected: BUILD SUCCESSFUL. If any unrelated test fails, that's pre-existing — capture which one and continue.

- [ ] **Step 8: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskCommand.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskCommandFactory.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/piimask/PiiMaskCommandTest.java
git commit -m "feat(piimask): wire piimask command into the registry"
```

---

## Task 11: SDK fluent helper

Adds `ZephFlow.piiMask(Config)` so users of the SDK can append the node like any other.

**Files:**
- Modify: `sdk/src/main/java/io/fleak/zephflow/sdk/ZephFlow.java`
- Create: `sdk/src/test/java/io/fleak/zephflow/sdk/PiiMaskSdkTest.java`

- [ ] **Step 1: Add the fluent method**

Open `sdk/src/main/java/io/fleak/zephflow/sdk/ZephFlow.java`.

1. Add the import near the other command DTO imports (alphabetical):
   ```java
   import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto;
   ```

2. Confirm `COMMAND_NAME_PII_MASK` is already statically imported via `MiscUtils.*`. If not, add it.

3. Add the method near the other intermediate-node helpers (after `eval(...)` or `sql(...)`):

```java
/**
 * Appends a PII masking node to the flow.
 *
 * @param config The PII mask configuration.
 * @return A new ZephFlow instance representing the flow with the node appended.
 */
public ZephFlow piiMask(PiiMaskCommandDto.Config config) {
  return appendNode(COMMAND_NAME_PII_MASK, config);
}
```

- [ ] **Step 2: Write failing SDK test**

`PiiMaskSdkTest.java` (modeled on `ZephFlowProcessTest`):

```java
package io.fleak.zephflow.sdk;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Config;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.DetectorConfig;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Detectors;
import io.fleak.zephflow.runner.DagResult;
import io.fleak.zephflow.runner.NoSourceDagRunner;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PiiMaskSdkTest {

  @Test
  void piiMaskInPipelineRedactsEmail() {
    Config cfg = new Config(
        List.of("$.message"),
        new Detectors(new DetectorConfig(null), null, null, null, null, null),
        null);

    DagResult result =
        ZephFlow.startFlow()
            .piiMask(cfg)
            .process(
                List.of(new Event("ping a@b.com please")),
                new NoSourceDagRunner.DagRunConfig(false, false));

    Map<String, List<RecordFleakData>> outputs = result.getOutputEvents();
    assertEquals(1, outputs.size(), "expected exactly one exit node");
    List<RecordFleakData> events = outputs.values().iterator().next();
    assertEquals(1, events.size());
    assertEquals("ping [EMAIL] please",
        ((Map<?, ?>) events.get(0).unwrap()).get("message"));
  }

  private record Event(String message) {}
}
```

- [ ] **Step 3: Run SDK test**

Run: `./gradlew :sdk:test --tests 'io.fleak.zephflow.sdk.PiiMaskSdkTest'`

Expected: PASS.

- [ ] **Step 4: Run full SDK + lib tests**

Run: `./gradlew :lib:test :sdk:test`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 5: Commit**

```bash
git add sdk/src/main/java/io/fleak/zephflow/sdk/ZephFlow.java \
        sdk/src/test/java/io/fleak/zephflow/sdk/PiiMaskSdkTest.java
git commit -m "feat(piimask): expose piiMask() fluent helper on ZephFlow"
```

---

## Done

After Task 11 the node is:

- Registered as `piimask` and discoverable by `DagExecutor`.
- Callable from JSON/YAML DAG definitions and from the SDK.
- Covered by tests for path parsing, leaf rewrites, each built-in pattern, masker logic (Luhn, IPv4 octets, ordering), parser, validator, and end-to-end command behavior.
- Emitting `pii_match_count` alongside the standard input/output/error counters.

Final verification:

```bash
./gradlew :api:test :lib:test :runner:test :sdk:test
```

Expected: BUILD SUCCESSFUL.
