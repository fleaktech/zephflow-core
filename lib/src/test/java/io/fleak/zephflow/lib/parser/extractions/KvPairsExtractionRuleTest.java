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
package io.fleak.zephflow.lib.parser.extractions;

import static io.fleak.zephflow.lib.utils.JsonUtils.loadFleakDataFromJsonResource;
import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_RAW;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.parser.CompiledRules;
import io.fleak.zephflow.lib.parser.ParserConfigCompiler;
import io.fleak.zephflow.lib.parser.ParserConfigs;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Unit tests for the public extract(String) method of KvPairsExtractionRule. */
class KvPairsExtractionRuleTest {

  @Nested
  @DisplayName("Standard Parsing with default separators (',' and '=')")
  class StandardParsingTests {

    private final ExtractionRule rule =
        new KvPairsExtractionRule(new KvPairExtractionConfig(",", "="));

    @Test
    @DisplayName("Should parse a simple, well-formed log string")
    void testExtract_SimpleWellFormedString() throws Exception {
      String input = "key1=value1,key2=\"value2\",key3=value3";
      RecordFleakData result = rule.extract(input);

      assertAll(
          "Should contain all key-value pairs",
          () -> assertEquals(3, result.unwrap().size()),
          () -> assertEquals("value1", result.unwrap().get("key1")),
          () -> assertEquals("value2", result.unwrap().get("key2")),
          () -> assertEquals("value3", result.unwrap().get("key3")));
    }

    @Test
    @DisplayName("Should handle values containing the pair separator")
    void testExtract_ValueWithPairSeparator() throws Exception {
      String input = "user=alice,permissions=\"read,write,execute\"";
      RecordFleakData result = rule.extract(input);

      assertAll(
          "Should correctly parse value with comma",
          () -> assertEquals(2, result.unwrap().size()),
          () -> assertEquals("alice", result.unwrap().get("user")),
          () -> assertEquals("read,write,execute", result.unwrap().get("permissions")));
    }

    @Test
    public void test() throws Exception {
      String raw =
          "software_version=\"14.1.0\",current_mitigation=\"alarm\",unit_hostname=\"f5networks.asm.test\",management_ip_address=\"10.192.138.11\",management_ip_address_2=\"\",operation_mode=\"Transparent\",date_time=\"2019-07-25 11:41:38\",policy_apply_date=\"2019-07-23 15:24:21\",policy_name=\"/Common/extranet_sonstige\",vs_name=\"/Common/extranet-t.qradar.example.test_443\",anomaly_attack_type=\"Distributed Attack\",uri=\"/qradar.example.test\",attack_status=\"ongoing\",detection_mode=\"Number of Failed Logins Increased\",severity=\"Emergency\",mitigated_entity_name=\"username\",mitigated_entity_value=\"exnyjtgk\",mitigated_ipaddr_geo=\"N/A\",attack_id=\"2508639270\",mitigated_entity_failed_logins=\"0\",mitigated_entity_failed_logins_threshold=\"3\",mitigated_entity_total_mitigations=\"0\",mitigated_entity_passed_challenges=\"0\",mitigated_entity_passed_captchas=\"0\",mitigated_entity_rejected_logins=\"0\",leaked_username_login_attempts=\"0\",leaked_username_failed_logins=\"0\",leaked_username_time_of_last_login_attempt=\"2497667872\",normal_failed_logins=\"78\",detected_failed_logins=\"70\",failed_logins_threshold=\"100\",normal_login_attempts=\"91\",detected_login_attempts=\"78\",login_attempts_matching_leaked_credentials=\"0\",total_mitigated_login_attempts=\"60\",total_client_side_integrity_challenges=\"0\",total_captcha_challenges=\"0\",total_blocking_page_challenges=\"0\",total_passed_client_side_integrity_challenges=\"0\",total_passed_captcha_challenges=\"0\",total_drops=\"0\",total_successful_mitigations=\"0\",protocol=\"HTTPS\",login_attempts_matching_leaked_credentials_threshold=\"100\",login_stress=\"73\"";
      RecordFleakData result = rule.extract(raw);
      System.out.println(toJsonString(result));
    }

    @Test
    @DisplayName("Should handle pairs with quoted keys")
    void testExtract_QuotedKey() throws Exception {
      String input = "\"key1\"=value1,key2=value2";
      RecordFleakData result = rule.extract(input);

      assertAll(
          "Should correctly parse quoted key",
          () -> assertEquals(2, result.unwrap().size()),
          () -> assertEquals("value1", result.unwrap().get("key1")),
          () -> assertEquals("value2", result.unwrap().get("key2")));
    }

    @Test
    @DisplayName("Should handle values containing the key-value separator")
    void testExtract_ValueWithKvSeparator() throws Exception {
      String input = "id=123,query=\"SELECT * FROM users WHERE name='bob'\"";
      RecordFleakData result = rule.extract(input);

      assertAll(
          "Should correctly parse value with equals sign",
          () -> assertEquals(2, result.unwrap().size()),
          () -> assertEquals("123", result.unwrap().get("id")),
          () -> assertEquals("SELECT * FROM users WHERE name='bob'", result.unwrap().get("query")));
    }

    @Test
    @DisplayName("Should handle values with escaped quotes")
    void testExtract_ValueWithEscapedQuotes() throws Exception {
      String input = "id=456,json=\"{\\\"user\\\":\\\"charlie\\\"}\"";
      RecordFleakData result = rule.extract(input);

      assertEquals("{\"user\":\"charlie\"}", result.unwrap().get("json"));
    }

    @Test
    @DisplayName("Should handle leading/trailing whitespace around pairs and values")
    void testExtract_WithExtraWhitespace() throws Exception {
      String input = "  key1 = value1 ,  key2= \" value2 \"  ";
      RecordFleakData result = rule.extract(input);

      assertAll(
          "Should trim whitespace correctly",
          () -> assertEquals(2, result.unwrap().size()),
          () -> assertEquals("value1", result.unwrap().get("key1")),
          () ->
              assertEquals(
                  " value2 ", result.unwrap().get("key2")) // Whitespace inside quotes is preserved
          );
    }
  }

  @Nested
  @DisplayName("Handling of Malformed and Edge Case Inputs")
  class MalformedInputTests {

    private final ExtractionRule rule =
        new KvPairsExtractionRule(new KvPairExtractionConfig(",", "="));

    @Test
    @DisplayName("Should return an empty map for null, empty, or blank input")
    void testExtract_NullOrEmptyInput() throws Exception {
      assertTrue(
          rule.extract(null).unwrap().isEmpty(), "Null input should result in an empty map.");
      assertTrue(
          rule.extract("").unwrap().isEmpty(), "Empty string input should result in an empty map.");
      assertTrue(
          rule.extract("   ").unwrap().isEmpty(),
          "Blank string input should result in an empty map.");
    }

    @Test
    @DisplayName("Should skip pairs where the separator is inside quotes")
    void testExtract_SkipsPairWithInternalSeparator() {
      String input = "key1=value1,\"key2=value2\"";

      Exception e = assertThrows(IllegalArgumentException.class, () -> rule.extract(input));
      assertEquals("no valid key-value separator found: \"key2=value2\"", e.getMessage());
    }

    @Test
    @DisplayName("Should handle empty values correctly")
    void testExtract_EmptyValues() throws Exception {
      String input = "key1=,key2=\"\",key3=value3";
      RecordFleakData result = rule.extract(input);

      assertAll(
          "Should handle various empty value formats",
          () -> assertEquals(3, result.unwrap().size()),
          () -> assertEquals("", result.unwrap().get("key1")),
          () -> assertEquals("", result.unwrap().get("key2")),
          () -> assertEquals("value3", result.unwrap().get("key3")));
    }

    @Test
    @DisplayName("Should skip pairs without a key-value separator")
    void testExtract_SkipsPairWithoutSeparator() {
      String input = "key1,key2=value2";
      Exception e = assertThrows(IllegalArgumentException.class, () -> rule.extract(input));
      assertEquals("no valid key-value separator found: key1", e.getMessage());
    }
  }

  @Nested
  @DisplayName("Parsing with Custom Separators")
  class CustomSeparatorTests {

    @Test
    @DisplayName("Should work correctly with pipe and colon separators")
    void testExtract_WithPipeAndColonSeparators() throws Exception {
      // Use pipe '|' to separate pairs and colon ':' to separate key/value
      ExtractionRule rule = new KvPairsExtractionRule(new KvPairExtractionConfig("|", ":"));
      String input = "user:alice|role:\"admin|manager\"|id:1234";
      RecordFleakData result = rule.extract(input);

      assertAll(
          "Should parse correctly with custom separators",
          () -> assertEquals(3, result.unwrap().size()),
          () -> assertEquals("alice", result.unwrap().get("user")),
          () -> assertEquals("admin|manager", result.unwrap().get("role")),
          () -> assertEquals("1234", result.unwrap().get("id")));
    }

    @Test
    @DisplayName("Should work with multi-character separators")
    void testExtract_WithMultiCharSeparators() throws Exception {
      // Use " | " to separate pairs and "=>" to separate key/value
      ExtractionRule rule = new KvPairsExtractionRule(new KvPairExtractionConfig(" | ", "=>"));
      String input = "user=>alice | role=>admin | status=>active";
      RecordFleakData result = rule.extract(input);

      assertAll(
          "Should parse correctly with multi-char separators",
          () -> assertEquals(3, result.unwrap().size()),
          () -> assertEquals("alice", result.unwrap().get("user")),
          () -> assertEquals("admin", result.unwrap().get("role")),
          () -> assertEquals("active", result.unwrap().get("status")));
    }

    @Test
    @DisplayName("Should work with escape sequences like tab")
    void testExtract_WithEscapeSequence() throws Exception {
      // Use \\t which should be unescaped to actual tab
      ExtractionRule rule = new KvPairsExtractionRule(new KvPairExtractionConfig("\\t", "="));
      String input = "key1=value1\tkey2=value2\tkey3=value3";
      RecordFleakData result = rule.extract(input);

      assertAll(
          "Should parse tab-separated values",
          () -> assertEquals(3, result.unwrap().size()),
          () -> assertEquals("value1", result.unwrap().get("key1")),
          () -> assertEquals("value2", result.unwrap().get("key2")),
          () -> assertEquals("value3", result.unwrap().get("key3")));
    }
  }

  @Test
  void extract() throws IOException {

    String raw =
        """
date=2025-07-25 time=07:43:43 devname="FortiGate-40F-SVA" devid="FGT40FTK2409BDPZ" eventtime=1753454623318460180 tz="-0700" logid="0001000014" type="traffic" subtype="local" level="notice" vd="root" srcip=fe80::2a70:4eff:fe71:34f1 srcport=5353 srcintf="wan" srcintfrole="wan" dstip=ff02::fb dstport=5353 dstintf="root" dstintfrole="undefined" sessionid=99717 proto=17 action="deny" policyid=0 policytype="local-in-policy6" service="udp/5353" trandisp="noop" app="udp/5353" duration=0 sentbyte=0 rcvdbyte=0 sentpkt=0 rcvdpkt=0 appcat="unscanned\"""";

    ParserConfigs.ParserConfig parserConfig =
        ParserConfigs.ParserConfig.builder()
            .targetField(FIELD_NAME_RAW)
            .extractionConfig(
                KvPairExtractionConfig.builder().kvSeparator("=").pairSeparator(" ").build())
            .build();
    CompiledRules.ParseRule parseRule = new ParserConfigCompiler().compile(parserConfig);
    RecordFleakData input = (RecordFleakData) FleakData.wrap(Map.of(FIELD_NAME_RAW, raw));
    FleakData actual = parseRule.parse(input);
    FleakData expected =
        loadFleakDataFromJsonResource("/parser/fortinet_traffic_fortigate_parsed.json");
    assertEquals(expected, actual);
  }
}
