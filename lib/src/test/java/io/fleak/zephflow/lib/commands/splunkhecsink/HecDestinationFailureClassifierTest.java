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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.splunkhecsink.SplunkHecSinkFlusher.HecResponseException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.Test;

class HecDestinationFailureClassifierTest {

  private static HecResponseException hec(int status, int code) {
    return HecResponseException.fromResponse(
        status, "{\"text\":\"HEC says no\",\"code\":" + code + "}");
  }

  @Test
  void classifiesByStatusAndHecCode() {
    Map<String, Throwable> failures = new LinkedHashMap<>();
    failures.put("IOException", new IOException("connection refused"));
    failures.put("SSLHandshakeException", new SSLHandshakeException("No subject alternative name"));
    failures.put("InterruptedException", new InterruptedException("stopping"));
    failures.put("RuntimeException", new RuntimeException("unexpected"));
    failures.put("408", hec(408, 0));
    failures.put("429", hec(429, 26));
    failures.put("500", hec(500, 8));
    failures.put("503", hec(503, 9));
    failures.put("401", hec(401, 2));
    failures.put("403", hec(403, 4));
    failures.put("404", hec(404, 7));
    failures.put("413", hec(413, 27));
    failures.put("400 code 7", hec(400, 7));
    failures.put(
        "400 code 7 record index",
        HecResponseException.fromResponse(
            400,
            "{\"text\":\"Incorrect index\",\"code\":7,\"invalid-event-number\":1}",
            List.of(
                new RecordFleakData(
                    Map.of(FIELD_NAME_INDEX, new StringPrimitiveFleakData("mian"))))));
    failures.put("400 code 5", hec(400, 5));
    failures.put("400 code 6", hec(400, 6));
    failures.put("400 code 12", hec(400, 12));
    failures.put("400 code 13", hec(400, 13));
    failures.put("400 code 15", hec(400, 15));
    failures.put("400 non-JSON body", HecResponseException.fromResponse(400, "bad request"));
    failures.put(
        "400 code 6 non-numeric", HecResponseException.fromResponse(400, "{\"code\":\"6\"}"));
    failures.put("503 code 6", hec(503, 6));

    Map<String, Boolean> expected = new LinkedHashMap<>();
    expected.put("IOException", true);
    expected.put("SSLHandshakeException", true);
    expected.put("InterruptedException", true);
    expected.put("RuntimeException", true);
    expected.put("408", true);
    expected.put("429", true);
    expected.put("500", true);
    expected.put("503", true);
    expected.put("401", true);
    expected.put("403", true);
    expected.put("404", true);
    expected.put("413", false);
    expected.put("400 code 7", true);
    expected.put("400 code 7 record index", false);
    expected.put("400 code 5", false);
    expected.put("400 code 6", false);
    expected.put("400 code 12", false);
    expected.put("400 code 13", false);
    expected.put("400 code 15", false);
    expected.put("400 non-JSON body", true);
    expected.put("400 code 6 non-numeric", true);
    expected.put("503 code 6", true);

    HecDestinationFailureClassifier classifier = new HecDestinationFailureClassifier();
    Map<String, Boolean> actual = new LinkedHashMap<>();
    failures.forEach((name, failure) -> actual.put(name, classifier.isDestinationFailure(failure)));

    assertEquals(expected, actual);
  }

  @Test
  void responseExceptionKeepsStatusCodeAndReason() {
    Map<String, Parsed> expected =
        Map.of(
            "json",
            new Parsed(400, 6, "Splunk HEC error 400: Invalid data format", false),
            "non-json",
            new Parsed(503, null, "Splunk HEC error 503: busy", true),
            "empty",
            new Parsed(429, null, "Splunk HEC error 429: ", true));

    Map<String, Parsed> actual =
        Map.of(
            "json",
            parsed(
                HecResponseException.fromResponse(
                    400, "{\"text\":\"Invalid data format\",\"code\":6}")),
            "non-json",
            parsed(HecResponseException.fromResponse(503, "busy")),
            "empty",
            parsed(HecResponseException.fromResponse(429, null)));

    assertEquals(expected, actual);
  }

  private record Parsed(int status, Integer hecCode, String message, boolean retryable) {}

  private static Parsed parsed(HecResponseException e) {
    return new Parsed(e.getStatus(), e.getHecCode(), e.getMessage(), e.isRetryable());
  }
}
