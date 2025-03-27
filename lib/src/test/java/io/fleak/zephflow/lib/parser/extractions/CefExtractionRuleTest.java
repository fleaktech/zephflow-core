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

import java.util.Map;
import org.junit.jupiter.api.Test;

/** Created by bolei on 2/24/25 */
class CefExtractionRuleTest {
  @Test
  public void testExtract() throws Exception {
    String raw =
        "CEF:0|MCAS|SIEM_Agent|0.298.168|EVENT_CATEGORY_RUN_COMMAND|Run command|0|externalId=105693141_20893_0c180415-c5f1-4cf2-226a-08dd44be8110 rt=1738633915000 start=1738633915000 end=1738633915000 msg=Run command: task GATFRTokenIssue; Parameters: Session ID suser=abc@xyz.com destinationServiceName=Microsoft Exchange Online dvc= requestClientApplication= cs1Label=portalURL cs1=https://security.microsoft.com/cloudapps/activity-log?activity.id=eq(105693141_20893_0c180415-c5f1-4cf2-226a-08dd44be8110,) cs2Label=uniqueServiceAppIds cs2=APPID_OUTLOOK cs3Label=targetObjects cs3=,GATFRTokenIssue,abc,abc cs4Label=policyIDs cs4= c6a1Label=Device IPv6 Address c6a1=";
    CefExtractionRule rule = new CefExtractionRule();
    Map<String, Object> result = rule.extract(raw).unwrap();
    assertEquals("MCAS", result.get("deviceVendor"));
    assertTrue(result.get("c6a1").toString().isEmpty()); // empty value field
    assertEquals("portalURL", result.get("cs1Label"));
    assertEquals(result.get("cs1"), result.get("portalURL"));
    assertEquals(
        "https://security.microsoft.com/cloudapps/activity-log?activity.id=eq(105693141_20893_0c180415-c5f1-4cf2-226a-08dd44be8110,)",
        result.get("cs1"));
  }
}
