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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.utils.MiscUtils;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

/** Created by bolei on 5/8/25 */
class DelimitedTextExtractionRuleTest {

  @Test
  void extract() throws Exception {
    String log = MiscUtils.loadStringFromResource("/parser/snare_windows.txt");

    List<String> columns =
        List.of(
            "Hostname",
            "SnareLogType",
            "Criticality",
            "EventLogSource",
            "SnareEventCounter",
            "SubmitTime",
            "EventCode",
            "SourceName",
            "UserName",
            "SIDType",
            "WindowsLogType",
            "ComputerName",
            "CategoryString",
            "DataString",
            "Message",
            "EventLogCounter");

    DelimitedTextExtractionRule delimitedTextExtractionRule =
        new DelimitedTextExtractionRule("\t", columns);
    RecordFleakData parsed = delimitedTextExtractionRule.extract(log);
    assertEquals(
        ImmutableMap.builder()
            .put("Hostname", "generic-server.example.com")
            .put("SnareLogType", "MSWinEventLog")
            .put("Criticality", "0")
            .put("EventLogSource", "Security")
            .put("SnareEventCounter", "[LogEntryID]")
            .put("SubmitTime", "[Timestamp]")
            .put("EventCode", "4657")
            .put("SourceName", "Microsoft-Windows-Security-Auditing")
            .put("UserName", "EXAMPLE_DOMAIN\\GENERIC-SERVER$")
            .put("SIDType", "N/A")
            .put("WindowsLogType", "Success Audit")
            .put("ComputerName", "generic-server.example.com")
            .put("CategoryString", "Registry")
            .put("DataString", "")
            .put(
                "Message",
                "A registry value was modified.  Subject: Security ID: S-1-5-18 Account Name: GENERIC-SERVER$ Account Domain: EXAMPLE_DOMAIN Logon ID: [LogonID]  Object: Object Name: \\REGISTRY\\MACHINE\\SOFTWARE\\Vendor\\ProductName\\Status Object Value Name: LOG_SETTING_NAME Handle ID: [HandleID] Operation Type: Existing registry value modified  Process Information: Process ID: [ProcessID] Process Name: C:\\Program Files\\ApplicationFolder\\Application.exe  Change Information: Old Value Type: REG_SZ Old Value: <BookmarkList>  <Bookmark Channel='Security' RecordId='[OldRecordID]' IsCurrent='true'/> </BookmarkList> New Value Type: REG_SZ New Value: <BookmarkList>  <Bookmark Channel='Security' RecordId='[NewRecordID]' IsCurrent='true'/> </BookmarkList>")
            .put("EventLogCounter", "0")
            .build(),
        parsed.unwrap());
  }

  @Test
  @DisplayName(
      "Should correctly parse a row from a CSV file with special characters and quoted headers")
  void processCsvFileWithSpecialCharacters() {
    String resourcePath = "/parser/csv_file_with_special_chars.csv";

    try (InputStream is = this.getClass().getResourceAsStream(resourcePath)) {
      if (is == null) {
        fail("Cannot find resource file: " + resourcePath);
      }

      List<String> lines =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8)).lines().toList();

      String columnStr = lines.get(0);
      List<String> columns = DelimitedTextExtractionRule.parseColumnNames(columnStr);
      String dataRow = lines.get(1);

      DelimitedTextExtractionRule rule =
          DelimitedTextExtractionRule.createDelimitedTextExtractionRule(",", columnStr);

      RecordFleakData result = rule.extract(dataRow);
      Map<String, Object> payload = result.unwrap();

      assertNotNull(payload, "The parsed payload should not be null.");
      assertEquals(
          columns.size(),
          payload.size(),
          "The number of parsed fields should match the number of columns.");

      // Verify a few key fields to ensure correct parsing
      assertEquals("12345", payload.get("product_id"));
      assertEquals(
          "Panasonic Lumix G97 Mirrorless Camera with 12-60mm f/3.5-5.6 Lens", payload.get("name"));
      assertEquals(
          "<b>Upgraded G95 Camera</b>: Now with USB-C, a better screen & Bluetooth 5.0. Still has a 20.3MP sensor, 5-axis stabilization, and 4K video.",
          payload.get("description"));
      assertEquals("$847.99", payload.get("price"));

      // Crucially, test the field with the quoted header containing a comma
      assertEquals("special, value", payload.get("special, field"));

    } catch (Exception e) {
      fail("Test failed due to an exception: " + e.getMessage());
    }
  }
}
