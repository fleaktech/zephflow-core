/**
 * Copyright 2025 Fleak Tech Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    PiiPathWriter.RewriteResult r = PiiMasker.mask("ok 192.168.0.1 bad 999.1.1.1", List.of(ip));
    assertEquals("ok [IPV4] bad 999.1.1.1", r.output());
    assertEquals(1, r.replacementCount());
  }

  @Test
  void detectorsApplyInDeclaredOrder() {
    // custom first, then email — custom replacement is opaque to the email detector
    PiiMasker.Spec custom = PiiMasker.custom("override", Pattern.compile("a@b\\.com"), "<custom>");
    PiiMasker.Spec email = PiiMasker.builtIn(BuiltInDetectors.EMAIL, null);
    PiiPathWriter.RewriteResult r = PiiMasker.mask("a@b.com and c@d.com", List.of(custom, email));
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
