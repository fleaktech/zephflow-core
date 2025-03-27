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
package io.fleak.zephflow.sdk;

import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonString;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.lib.commands.SimpleHttpClient;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** Created by bolei on 3/12/25 */
public class ZephFlowApiSubmissionTest {
  /**
   * use the SDK to define a DAG
   *
   * <p>- submit it to HTTP server to create a workflow API backend
   *
   * <p>- invoke the newly created API
   */
  @Disabled
  @Test
  public void testSubmitWorkflowAsApiBackend() throws URISyntaxException {
    ZephFlow flow = ZephFlow.startFlow();
    ZephFlow evenFlow = flow.filter("$.num%2 == 0").eval("dict(num=$.num, label='even')");
    ZephFlow oddFlow = flow.filter("$.num%2 == 1").eval("dict(num=$.num, label='odd')");
    String responseStr =
        ZephFlow.merge(evenFlow, oddFlow).submitApiEndpoint("http://localhost:8080/");
    Map<String, Object> responseMap = fromJsonString(responseStr, new TypeReference<>() {});
    assert responseMap != null;
    String workflowId = (String) responseMap.get("id");

    String inputDataPayload =
        """
[
  {
    "num": 0
  },
  {
    "num": 1
  }
]""";

    SimpleHttpClient httpClient =
        SimpleHttpClient.getInstance(SimpleHttpClient.MAX_RESPONSE_SIZE_BYTES);
    String outputEvents =
        httpClient.callHttpEndpointNoSecureCheck(
            "http://localhost:8080/api/v1/execution/run/" + workflowId + "/batch",
            SimpleHttpClient.HttpMethodType.POST,
            inputDataPayload,
            List.of("Content-Type: application/json"));
    System.out.println(outputEvents);
  }
}
