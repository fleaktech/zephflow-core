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
package io.fleak.zephflow.lib.commands.eval;

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static io.fleak.zephflow.lib.utils.JsonUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.ScalarCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Created by bolei on 10/20/24 */
class EvalCommandTest {

  @Test
  public void testArrayFunction() throws IOException {
    String evalExpr =
        """
dict(
    attachments=array($.attachments.snmp_pdf, $.attachments.f1)
)""";
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/eval/eval_test_1.json");
    FleakData expected = FleakData.wrap(Map.of("attachments", List.of("s3://a.pdf", "s3://b.pdf")));
    assert expected != null;
    testEval(inputEvent, evalExpr, expected);

    evalExpr =
        """
dict(
    attachments=array()
)""";
    expected = FleakData.wrap(Map.of("attachments", List.of()));
    assert expected != null;
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testArrayForEach() throws IOException {
    String evalExpr =
        """
dict(
    devices=arr_foreach(
        $.resp.Test1,
        elem,
        dict(
            osVersion=elem.operation_system,
            source=$.integration,
            pdf_attachment=$.attachments.snmp_pdf,
            ip=elem.ipAddr
        )
    )
)""";
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/eval/eval_test_1.json");
    FleakData expected =
        loadFleakDataFromJsonResource("/eval/eval_test_1_expected_array_foreach_dict.json");
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testNestedArrayForEach() throws IOException {
    String evalExpr =
        """
dict(
  val = arr_foreach($.numbers, groupElem,
      arr_foreach(groupElem.values, valueElem,
          dict(
              groupName = groupElem.group,
              value = valueElem
          )
      )
  )
)
""";
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/eval/eval_test_2.json");
    FleakData expected =
        loadFleakDataFromJsonResource("/eval/eval_test_2_expected_nested_arr_foreach.json");
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testDict() throws IOException {
    String evalExpr =
        """
dict(
  city=$.address.city,
  state=$.address.state,
  foo= dict(
    tag0=$.tags[0],
    tag1=$.tags[1]
  )
)
""";

    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/eval/eval_test_3.json");
    FleakData expected = loadFleakDataFromJsonResource("/eval/eval_test_3_expected_dict.json");
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testArrFlatten() throws IOException {
    String evalExpr =
        """
dict(
  val = arr_flatten(
    arr_foreach($.numbers, groupElem,
      arr_foreach(groupElem.values, valueElem,
          dict(
              groupName = groupElem.group,
              value = valueElem
          )
      )
    )
  )
)
""";
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/eval/eval_test_2.json");
    FleakData expected = loadFleakDataFromJsonResource("/eval/eval_test_2_expected_flatten.json");
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testArrFlattenMixedInput() {
    String evalExpr = "dict(result = arr_flatten($.f))";
    RecordFleakData inputEvent =
        (RecordFleakData)
            loadFleakDataFromJsonString(
                """
            {"f": [[1, 2], 3, [4, 5]]}
            """);
    FleakData expected = FleakData.wrap(Map.of("result", List.of(1, 2, 3, 4, 5)));
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testGrok() throws IOException {
    String evalExpr =
        """
dict(
  grok_result = grok($.__raw__, "%{GREEDYDATA:timestamp} %{HOSTNAME:hostname} %{WORD:program}\\\\[%{POSINT:pid}\\\\]: %ASA-%{INT:level}-%{INT:message_number}: %{GREEDYDATA:message_text}")
)
""";
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_305011_parsed.json");

    FleakData expected = loadFleakDataFromJsonResource("/eval/eval_test_grok_expected.json");
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testDictMerge() throws IOException {
    String evalExpr =
        """
dict_merge(
  $,
  grok($.__raw__, "%{GREEDYDATA:timestamp} %{HOSTNAME:hostname} %{WORD:program}\\\\[%{POSINT:pid}\\\\]: %ASA-%{INT:level}-%{INT:message_number}: %{GREEDYDATA:message_text}")
)
""";

    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_305011_raw.json");
    FleakData expected = loadFleakDataFromJsonResource("/eval/eval_test_dict_merge_expected.json");
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testDurationStrToMills() throws IOException {
    String evalExpr =
        """
dict(
  duration=duration_str_to_mills($.duration_str)
)
""";
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/eval/eval_test_3.json");
    FleakData expected =
        loadFleakDataFromJsonResource("/eval/eval_test_duration_str_to_millis_expected.json");
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testCaseWhen() throws IOException {

    // should match $.age > 21 first and return
    String evalExpr =
        """
dict(
  duration = case(
    $.age > 21 => duration_str_to_mills($.duration_str),
    $.age > 20 => str_split($.address.city, ' '),
    _ => 100
  )
)
""";
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/eval/eval_test_3.json");

    FleakData expected =
        loadFleakDataFromJsonResource("/eval/eval_test_duration_str_to_millis_expected.json");
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testNull() {
    String evalExpr =
        """
dict(
  val = case(
    $.k > 21 => true,
    _ => null
  )
)
""";
    Map<String, Object> payload = new HashMap<>();
    payload.put("k", null);
    RecordFleakData inputEvent = (RecordFleakData) FleakData.wrap(payload);

    Map<String, Object> expectedPayload = new HashMap<>();
    expectedPayload.put("val", null);
    FleakData expected = FleakData.wrap(expectedPayload);
    assert expected != null;
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testNull2() {
    String evalExpr =
        """
dict(
  val = case(
    $.k == null => true,
    _ => false
  )
)
""";
    Map<String, Object> payload = new HashMap<>();
    payload.put("k", null);
    RecordFleakData inputEvent = (RecordFleakData) FleakData.wrap(payload);
    FleakData expected = FleakData.wrap(Map.of("val", true));
    assert expected != null;
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testNull3() {
    String evalExpr =
        """
dict(
  val = $.securityContext.domain
)
""";
    RecordFleakData inputEvent =
        (RecordFleakData)
            FleakData.wrap(
                fromJsonString(
                    """
  {
  "securityContext": {
    "isp": "google",
    "domain": null
  }
}
""",
                    new TypeReference<Map<String, Object>>() {}));
    Map<String, Object> expectedPayload = new HashMap<>();
    expectedPayload.put("val", null);
    FleakData expected = FleakData.wrap(expectedPayload);
    assert expected != null;
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testUpper() {
    String evalExpr =
        """
dict(
  val = upper($.k)
)
""";
    RecordFleakData inputEvent = (RecordFleakData) FleakData.wrap(Map.of("k", "abc"));
    FleakData expected = FleakData.wrap(Map.of("val", "ABC"));
    assert expected != null;
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testLower() {
    String evalExpr =
        """
dict(
  val = lower($.k)
)
""";
    RecordFleakData inputEvent = (RecordFleakData) FleakData.wrap(Map.of("k", "ABC"));
    FleakData expected = FleakData.wrap(Map.of("val", "abc"));
    assert expected != null;
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testToString() {
    String evalExpr =
        """
dict(
  val = to_str($.k)
)
""";
    RecordFleakData inputEvent = (RecordFleakData) FleakData.wrap(Map.of("k", 123));
    FleakData expected = FleakData.wrap(Map.of("val", "123"));
    assert expected != null;
    testEval(inputEvent, evalExpr, expected);

    inputEvent = (RecordFleakData) FleakData.wrap(Map.of("kk", 123));
    Map<String, Object> expectedPayload = new HashMap<>();
    expectedPayload.put("val", null);
    expected = FleakData.wrap(expectedPayload);
    assert expected != null;
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testStrContains() {
    String evalExpr =
        """
dict(
  contains = str_contains($.k, $.sub_str)
)
""";
    RecordFleakData inputEvent =
        (RecordFleakData) FleakData.wrap(Map.of("k", "foo", "sub_str", "oo"));
    FleakData expected = FleakData.wrap(Map.of("contains", true));
    assert expected != null;
    testEval(inputEvent, evalExpr, expected);

    inputEvent = (RecordFleakData) FleakData.wrap(Map.of("k", "foo", "sub_str", "xyz"));
    expected = FleakData.wrap(Map.of("contains", false));
    assert expected != null;
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testSizeFunction() {
    String evalExpr =
        """
dict(
  s = size_of($.k)
)
""";
    RecordFleakData inputEvent = (RecordFleakData) FleakData.wrap(Map.of("k", "foo"));
    FleakData expected = FleakData.wrap(Map.of("s", 3));
    assert expected != null;
    testEval(inputEvent, evalExpr, expected);

    inputEvent = (RecordFleakData) FleakData.wrap(Map.of("k", List.of()));
    expected = FleakData.wrap(Map.of("s", 0));
    assert expected != null;
    testEval(inputEvent, evalExpr, expected);

    inputEvent = (RecordFleakData) FleakData.wrap(Map.of("k", List.of(1)));
    expected = FleakData.wrap(Map.of("s", 1));
    assert expected != null;
    testEval(inputEvent, evalExpr, expected);

    inputEvent = (RecordFleakData) FleakData.wrap(Map.of("k", Map.of()));
    expected = FleakData.wrap(Map.of("s", 0));
    assert expected != null;
    testEval(inputEvent, evalExpr, expected);

    inputEvent = (RecordFleakData) FleakData.wrap(Map.of("k", Map.of("kk", "vv")));
    expected = FleakData.wrap(Map.of("s", 1));
    assert expected != null;
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testMappingOcsf_CiscoAsa106023() throws IOException {
    String evalExpr =
        """
dict(
    activity_id = 5,
    activity_name = "Refuse",
    category_name = "Network Activity",
    category_uid = 4,
    class_name = "Network Activity",
    class_uid = 4001,
    time = ts_str_to_epoch($.timestamp, "MMM dd yyyy HH:mm:ss"),
    raw_data = $.__raw__,
    src_endpoint = dict(
      ip = $.source_ip,
      port = parse_int($.source_port),
      interface_name = $.source_interface
    ),
    dst_endpoint = dict(
      ip = $.dest_ip,
      port = parse_int($.dest_port),
      interface_name = $.dest_interface
    ),
    connection_info = dict(
      boundary = "External",
      boundary_id = 3,
      direction = "Inbound",
      direction_id = 1,
      protocol_name = $.protocol
    ),
    metadata= dict(
      event_code = $.message_number,
      original_time=$.timestamp,
      product = dict(
        vendor_name = "Cisco",
        name = "Cisco ASA",
        version = "Unknown"
      ),
      version = "1.3.0"
    ),
    severity_id =5,
    severity = "Critical",
    type_name = "Network Activity: Refuse",
    type_uid = 400105,
    status = "Failure",
    status_id = 2,
    unmapped = dict (
      rule_ids =  $.rule_ids,
      program = $.program,
      pid = $.pid
     )
)""";
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_106023_parsed.json");

    FleakData expected = loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_106023_ocsf.json");
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testMappingOcsf_CiscoAsa302013() throws IOException {
    String evalExpr =
        """
dict(
    activity_id = 1,
    activity_name = "Open",
    category_name = "Network Activity",
    category_uid = 4,
    class_name = "Network Activity",
    class_uid = 4001,
    time = ts_str_to_epoch($.timestamp, "MMM dd yyyy HH:mm:ss"),
    raw_data = $.__raw__,
    src_endpoint = dict(
      ip = $.source_ip,
      port = parse_int($.source_port),
      interface_name = $.source_interface
    ),
    dst_endpoint = dict(
      ip = $.dest_ip,
      port = parse_int($.dest_port),
      interface_name = $.dest_interface
    ),
    connection_info = dict(
      boundary = "External",
      boundary_id = 3,
      direction = "Outbound",
      direction_id = 2,
      protocol_name = $.protocol,
      uid = $.connection_id
    ),
    severity_id =1,
    severity = "Informational",
    status = "Success",
    status_id = 1,
    type_name = "Network Activity: Open",
    type_uid = 400101,
    metadata= dict(
      event_code = $.message_number,
      original_time=$.timestamp,
      product = dict(
        vendor_name = "Cisco",
        name = "Cisco ASA",
        version = "Unknown"
      ),
      version = "1.3.0"
    ),
    unmapped = dict (
      program = $.program,
      pid = $.pid
    )
)""";
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_302013_parsed.json");

    FleakData expected = loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_302013_ocsf.json");
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testMappingOcsf_CiscoAsa302014() throws IOException {
    String evalExpr =
        """
dict(
    activity_id = 3,
    activity_name = "Reset",
    category_name = "Network Activity",
    category_uid = 4,
    class_name = "Network Activity",
    class_uid = 4001,
    time = ts_str_to_epoch($.timestamp, "MMM dd yyyy HH:mm:ss"),
    raw_data = $.__raw__,
    src_endpoint = dict(
      ip = $.source_ip,
      port = parse_int($.source_port),
      interface_name = $.source_interface
    ),
    dst_endpoint = dict(
      ip = $.dest_ip,
      port = parse_int($.dest_port),
      interface_name = $.dest_interface
    ),
    connection_info = dict(
      boundary = "External",
      boundary_id = 3,
      direction = "Inbound",
      direction_id = 1,
      protocol_name = $.protocol,
      uid = $.connection_id
    ),
    severity_id =1,
    severity = "Informational",
    type_name = "Network Activity: Reset",
    type_uid = 400103,
    duration = duration_str_to_mills($.duration),
    status_id = 2,
    status = "Failure",
    status_detail = $.reason,
    traffic = dict(
      bytes = parse_int($.bytes)
    ),
    metadata= dict(
      event_code = $.message_number,
      original_time=$.timestamp,
      product = dict(
        vendor_name = "Cisco",
        name = "Cisco ASA",
        version = "Unknown"
      ),
      version = "1.3.0"
    ),
    unmapped = dict (
      program = $.program,
      pid = $.pid
    )
)""";
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_302014_parsed.json");

    FleakData expected = loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_302014_ocsf.json");
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testMappingOcsf_CiscoAsa302015() throws IOException {
    String evalExpr =
        """
dict(
    activity_id = 1,
    activity_name = "Open",
    category_name = "Network Activity",
    category_uid = 4,
    class_name = "Network Activity",
    class_uid = 4001,
    time = ts_str_to_epoch($.timestamp, "MMM dd yyyy HH:mm:ss"),
    raw_data = $.__raw__,
    src_endpoint = dict(
      ip = $.source_ip,
      port = parse_int($.source_port),
      interface_name = $.source_interface
    ),
    dst_endpoint = dict(
      ip = $.dest_ip,
      port = parse_int($.dest_port),
      interface_name = $.dest_interface
    ),
    connection_info = dict(
      boundary = "External",
      boundary_id = 3,
      direction = "Outbound",
      direction_id = 2,
      protocol_name = $.protocol,
      uid = $.connection_id
    ),
    severity_id =1,
    severity = "Informational",
    status = "Success",
    status_id = 1,
    type_name = "Network Activity: Open",
    type_uid = 400101,
    metadata= dict(
      event_code = $.message_number,
      original_time=$.timestamp,
      product = dict(
        vendor_name = "Cisco",
        name = "Cisco ASA",
        version = "Unknown"
      ),
      version = "1.3.0"
    ),
    unmapped = dict (
      program = $.program,
      pid = $.pid
    )
)""";
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_302015_parsed.json");

    FleakData expected = loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_302015_ocsf.json");
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testMappingOcsf_CiscoAsa302016() throws IOException {
    String evalExpr =
        """
dict(
    activity_id = 2,
    activity_name = "Close",
    category_name = "Network Activity",
    category_uid = 4,
    class_name = "Network Activity",
    class_uid = 4001,
    time = ts_str_to_epoch($.timestamp, "MMM dd yyyy HH:mm:ss"),
    raw_data = $.__raw__,
    src_endpoint = dict(
      ip = $.source_ip,
      port = parse_int($.source_port),
      interface_name = $.source_interface
    ),
    dst_endpoint = dict(
      ip = $.dest_ip,
      port = parse_int($.dest_port),
      interface_name = $.dest_interface
    ),
    connection_info = dict(
      boundary = "External",
      boundary_id = 3,
      direction = "Inbound",
      direction_id = 1,
      protocol_name = $.protocol,
      uid = $.connection_id
    ),
    severity_id =1,
    severity = "Informational",
    type_name = "Network Activity: Close",
    type_uid = 400102,
    duration = duration_str_to_mills($.duration),
    status_id = 1,
    status = "Success",
    traffic = dict(
      bytes = parse_int($.bytes)
    ),
    metadata= dict(
      event_code = $.message_number,
      original_time=$.timestamp,
      product = dict(
        vendor_name = "Cisco",
        name = "Cisco ASA",
        version = "Unknown"
      ),
      version = "1.3.0"
    ),
    unmapped = dict (
      program = $.program,
      pid = $.pid
    )
)""";
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_302016_parsed.json");

    FleakData expected = loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_302016_ocsf.json");
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testMappingOcsf_CiscoAsa305011() throws IOException {
    String evalExpr =
        """
dict(
    activity_id = 1,
    activity_name = "Open",
    category_name = "Network Activity",
    category_uid = 4,
    class_name = "Network Activity",
    class_uid = 4001,
    time = ts_str_to_epoch($.timestamp, "MMM dd yyyy HH:mm:ss"),
    raw_data = $.__raw__,
    src_endpoint = dict(
      ip = $.source_ip,
      port = parse_int($.source_port),
      interface_name = $.source_zone
    ),
    dst_endpoint = dict(
      ip = $.dest_ip,
      port = parse_int($.dest_port),
      interface_name = $.dest_zone
    ),
    connection_info = dict(
      boundary = "External",
      boundary_id = 3,
      protocol_name = $.protocol,
      direction_id = 2,
      direction = "Outbound"
    ),
    severity_id = 1,
    severity = "Informational",
    status_id = 1,
    status = "Success",
    type_name = "Network Activity: Open",
    type_uid = 400101,
    metadata= dict(
      event_code = $.message_number,
      original_time=$.timestamp,
      product = dict(
        vendor_name = "Cisco",
        name = "Cisco ASA",
        version = "Unknown"
      ),
      version = "1.3.0"
    ),
    unmapped = dict(
      program = $.program,
      pid = $.pid
    )
)""";
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_305011_parsed.json");

    FleakData expected = loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_305011_ocsf.json");
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testMappingOcsf_CiscoAsa305012() throws IOException {
    String evalExpr =
        """
dict(
    activity_id = 2,
    activity_name = "Close",
    category_name = "Network Activity",
    category_uid = 4,
    class_name = "Network Activity",
    class_uid = 4001,
    time = ts_str_to_epoch($.timestamp, "MMM dd yyyy HH:mm:ss"),
    raw_data = $.__raw__,
    src_endpoint = dict(
      ip = $.source_ip,
      port = parse_int($.source_port),
      interface_name = $.source_interface
    ),
    dst_endpoint = dict(
      ip = $.dest_ip,
      port = parse_int($.dest_port),
      interface_name = $.dest_interface
    ),
    connection_info = dict(
      boundary = "External",
      boundary_id = 3,
      protocol_name = $.protocol,
      direction_id = 2,
      direction = "Outbound"
    ),
    duration = duration_str_to_mills($.duration),
    severity_id = 1,
    severity = "Informational",
    status_id = 1,
    status = "Success",
    type_name = "Network Activity: Close",
    type_uid = 400102,
    metadata= dict(
      event_code = $.message_number,
      original_time=$.timestamp,
      product = dict(
        vendor_name = "Cisco",
        name = "Cisco ASA",
        version = "Unknown"
      ),
      version = "1.3.0"
    ),
    unmapped = dict(
      program = $.program,
      pid = $.pid
    )
)""";
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_305012_parsed.json");

    FleakData expected = loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_305012_ocsf.json");
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testMappingOcsf_CiscoAsa113019() throws IOException {
    String evalExpr =
        """
dict(
   activity_id = 2,
   activity_name = "Close",
   category_name = "Network Activity",
   category_uid = 4,
   class_name = "Tunnel Activity",
   class_uid = 4014,
   time = ts_str_to_epoch($.timestamp, "MMM dd yyyy HH:mm:ss"),
   duration = duration_str_to_mills($.duration),
   raw_data = $.__raw__,
   user = dict(
     name = $.username,
     groups = array(
       dict(
         name = $.group_name
       )
     )
   ),
   src_endpoint = dict(
     ip = $.peer_ip
   ),
   connection_info = dict(
     boundary = "External",
     boundary_id = 3,
     direction_id = 1,
     direction = "Inbound",
     protocol_name = $.session_type
   ),
   severity_id = 2,
   severity = "Low",
   status_id = 2,
   status = "Failure",
   type_name = "Tunnel Activity: Close",
   type_uid = 401402,
   protocol_name = $.session_type,
   session = dict(
     is_vpn = true
   ),
   traffic = dict(
     bytes_out = $.bytes_xmt,
     bytes_in = $.bytes_rcv
   ),
   status_detail = $.disconnect_reason,
   metadata = dict(
     event_code = $.message_number,
     original_time = $.timestamp,
     product = dict(
       vendor_name = "Cisco",
       name = "Cisco ASA",
       version = "Unknown"
     ),
     version = "1.3.0"
   ),
   unmapped = dict(
     program = $.program,
     pid = $.pid
   )
 )""";
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_113019_parsed.json");

    FleakData expected = loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_113019_ocsf.json");
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testMappingOcsf_CiscoAsa113039() throws IOException {
    String evalExpr =
        """
dict(
   activity_id = 1,
   activity_name = "Open",
   category_name = "Network Activity",
   category_uid = 4,
   class_name = "Tunnel Activity",
   class_uid = 4014,
   time = ts_str_to_epoch($.timestamp, "MMM dd yyyy HH:mm:ss"),
   raw_data = $.__raw__,
   user = dict(
     name = $.username,
     groups = array(
       dict(
         name = $.group_name
       )
     )
   ),
   src_endpoint = dict(
     ip = $.ip_address
   ),
   connection_info = dict(
     boundary = "External",
     boundary_id = 3,
     direction_id = 1,
     direction = "Inbound"
   ),
   severity_id = 1,
   severity = "Informational",
   status_id = 1,
   status = "Success",
   type_name = "Tunnel Activity: Open",
   type_uid = 401401,
   session = dict(
     is_vpn = true
   ),
   metadata = dict(
     event_code = $.message_number,
     original_time = $.timestamp,
     product = dict(
       vendor_name = "Cisco",
       name = "Cisco ASA",
       version = "Unknown"
     ),
     version = "1.3.0"
   ),
   unmapped = dict(
     program = $.program,
     pid = $.pid
   )
 )""";
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_113039_parsed.json");

    FleakData expected = loadFleakDataFromJsonResource("/eval/cisco/cisco_asa_113039_ocsf.json");
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testEval_PythonFunc() {
    String evalExpr =
        """
dict(
  pyData1 = python(
    '
# Script 1
def process_req(req_value, factor):
  return (req_value + 1) * factor
',
    $.request.value,
    $.settings.factor
  ),
  pyData2 = python(
    '
# Script 2
def format_user(name, roles):
  return name.upper() + " roles: " + str(roles)
',
    $.user.name,
    $.user.roles
  ),
   pyData3 = python(
     '
# Script 3
def process_req(req_value, factor):
  return (req_value + 1) * factor
',
     10.0,
     2
   )
)
""";
    RecordFleakData inputEvent =
        (RecordFleakData)
            FleakData.wrap(
                Map.of(
                    "user", Map.of("name", "Alice", "id", 123L, "roles", List.of("admin", "dev")),
                    "request", Map.of("value", 99.5, "items", List.of("apple", "banana")),
                    "settings", Map.of("factor", 5)));
    FleakData expected =
        JsonUtils.loadFleakDataFromJsonString(
            """
{
  "pyData3": 22,
  "pyData1": 502.5,
  "pyData2": "ALICE roles: ['admin', 'dev']"
}
""");
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  void testEval_PythonFunc2() {
    String evalExpr =
        """
        dict(
          endpoint=arr_foreach(
            $.resp["assets"],
            elem,
            dict(
              test1=python(
                  'def test(arg1):
                      return arg1*2',
                  elem["deviceModel"]
                )
            )
          )
        )
        """;
    RecordFleakData inputEvent =
        (RecordFleakData)
            loadFleakDataFromJsonString(
                """
            {
              "resp": {
                "assets": [
                  {
                    "deviceModel": "a"
                  },
                  {
                    "deviceModel": "b"
                  }
                ]
              }
            }
            """);
    FleakData outputEvent =
        loadFleakDataFromJsonString(
            """
{"endpoint":[{"test1":"aa"},{"test1":"bb"}]}
""");
    testEval(inputEvent, evalExpr, outputEvent);
  }

  @Test
  public void testArrayZip() {
    String inputEventStr =
        """
{
  "TTLs": [
    300,
    140
  ],
  "answers": [
    "s3-1-w.amazonaws.com",
    "s3-w.us-east-1.amazonaws.com"
  ],
  "trans_id": 60300
}
""";
    RecordFleakData inputEvent = (RecordFleakData) loadFleakDataFromJsonString(inputEventStr);
    String expectedOutputEventStr =
        """
{
  "answers": [
    {
      "rdata": "s3-1-w.amazonaws.com",
      "ttl": 300,
      "packet_uid": 60300
    },
    {
      "rdata": "s3-w.us-east-1.amazonaws.com",
      "ttl": 140,
      "packet_uid": 60300
    }
  ]
}
""";
    FleakData expected = loadFleakDataFromJsonString(expectedOutputEventStr);
    String evalExpr =
        """
dict(
  answers=arr_foreach(range(size_of($.TTLs)), idx, dict(
    rdata=$.answers[idx],
    ttl=$.TTLs[idx],
    packet_uid=$.trans_id
  ))
)
""";
    testEval(inputEvent, evalExpr, expected);
  }

  @Test
  public void testOutputMultipleEvents() {
    String inputEventStr =
        """
        {
          "foo": 100,
          "arr":[
            {"k": 1},
            {"k": 2}
          ]
        }
        """;

    String evalExpr = "$.arr";
    RecordFleakData inputEvent = (RecordFleakData) loadFleakDataFromJsonString(inputEventStr);

    testEval(
        inputEvent,
        evalExpr,
        List.of(
            (RecordFleakData) FleakData.wrap(Map.of("k", 1)),
            (RecordFleakData) FleakData.wrap(Map.of("k", 2))));
  }

  @Test
  public void testOutputMultipleEvents_BadPath() {
    String inputEventStr =
        """
        {
          "arr":[
            {"k": 1},
            {"k": 2}
          ]
        }
        """;

    String evalExpr = "$.bad_field.some_field";
    RecordFleakData inputEvent = (RecordFleakData) loadFleakDataFromJsonString(inputEventStr);
    testEval(inputEvent, evalExpr, List.of());
  }

  @Test
  public void testOutputMultipleEvents_mixedArrayWithFailures() {
    String inputEventStr =
        """
        {
          "foo": 100,
          "arr":[
            {"k": 1},
            1,
            {"k": 2}
          ]
        }
        """;

    String evalExpr = "$.arr";
    RecordFleakData inputEvent = (RecordFleakData) loadFleakDataFromJsonString(inputEventStr);
    ScalarCommand.ProcessResult processResult = runEval(inputEvent, evalExpr);

    assertEquals(2, processResult.getOutput().size());
    assertEquals(
        toJsonString(List.of(FleakData.wrap(Map.of("k", 1)), FleakData.wrap(Map.of("k", 2)))),
        toJsonString(processResult.getOutput()));

    assertEquals(1, processResult.getFailureEvents().size());
    assertEquals(
        "failed to cast NumberPrimitiveFleakData(numberValue=1.0, numberType=LONG) into RecordFleakData",
        processResult.getFailureEvents().get(0).errorMessage());
  }

  private void testEval(RecordFleakData inputEvent, String evalExpr, FleakData expected) {
    testEval(inputEvent, evalExpr, List.of((RecordFleakData) expected));
  }

  private void testEval(
      RecordFleakData inputEvent, String evalExpr, List<RecordFleakData> expected) {
    ScalarCommand.ProcessResult processResult = runEval(inputEvent, evalExpr);
    assertTrue(processResult.getFailureEvents().isEmpty());
    assertEquals(toJsonString(expected), toJsonString(processResult.getOutput()));
  }

  private ScalarCommand.ProcessResult runEval(RecordFleakData inputEvent, String evalExpr) {
    EvalCommand evalCommand =
        (EvalCommand) new EvalCommandFactory().createCommand("my_node_id", JOB_CONTEXT);
    evalCommand.parseAndValidateArg(Map.of("expression", evalExpr));
    evalCommand.initialize(new MetricClientProvider.NoopMetricClientProvider());
    var context = evalCommand.getExecutionContext();
    ScalarCommand.ProcessResult processResult =
        evalCommand.process(List.of(inputEvent), "test_user", context);
    System.out.println(
        processResult.getFailureEvents().stream().map(ErrorOutput::errorMessage).toList());
    System.out.println(toJsonString(processResult.getOutput()));
    return processResult;
  }
}
