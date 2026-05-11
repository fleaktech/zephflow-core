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
