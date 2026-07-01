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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class S3EventNotificationParserTest {

  @Test
  void parse_directNotification() {
    String body =
        """
        {"Records":[{"eventName":"ObjectCreated:Put",
          "s3":{"bucket":{"name":"my-bucket"},"object":{"key":"path/to/file.json","size":42}}}]}
        """;
    assertEquals(List.of(new S3ObjectRef("my-bucket", "path/to/file.json")), parse(body));
  }

  @Test
  void parse_snsWrappedNotification() {
    String inner =
        "{\"Records\":[{\"eventName\":\"ObjectCreated:Post\","
            + "\"s3\":{\"bucket\":{\"name\":\"b\"},\"object\":{\"key\":\"k.csv\"}}}]}";
    String body =
        "{\"Type\":\"Notification\",\"TopicArn\":\"arn:aws:sns:us-east-1:123:topic\","
            + "\"Message\":"
            + quote(inner)
            + "}";
    assertEquals(List.of(new S3ObjectRef("b", "k.csv")), parse(body));
  }

  @Test
  void parse_testEventReturnsEmpty() {
    String body =
        """
        {"Service":"Amazon S3","Event":"s3:TestEvent","Time":"2025-01-01T00:00:00.000Z",
         "Bucket":"my-bucket","RequestId":"abc","HostId":"xyz"}
        """;
    assertEquals(List.of(), parse(body));
  }

  @Test
  void parse_objectRemovedSkipped() {
    String body =
        """
        {"Records":[{"eventName":"ObjectRemoved:Delete",
          "s3":{"bucket":{"name":"b"},"object":{"key":"gone.json"}}}]}
        """;
    assertEquals(List.of(), parse(body));
  }

  @Test
  void parse_urlEncodedKeys() {
    String body =
        """
        {"Records":[
          {"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"b"},"object":{"key":"a+b.json"}}},
          {"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"b"},"object":{"key":"dir/with%20space.json"}}},
          {"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"b"},"object":{"key":"100%25.json"}}}
        ]}
        """;
    assertEquals(
        List.of(
            new S3ObjectRef("b", "a b.json"),
            new S3ObjectRef("b", "dir/with space.json"),
            new S3ObjectRef("b", "100%.json")),
        parse(body));
  }

  @Test
  void parse_multipleRecordsMixedEvents() {
    String body =
        """
        {"Records":[
          {"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"b1"},"object":{"key":"k1"}}},
          {"eventName":"ObjectRemoved:Delete","s3":{"bucket":{"name":"b1"},"object":{"key":"k2"}}},
          {"eventName":"ObjectCreated:CompleteMultipartUpload","s3":{"bucket":{"name":"b2"},"object":{"key":"k3"}}}
        ]}
        """;
    assertEquals(List.of(new S3ObjectRef("b1", "k1"), new S3ObjectRef("b2", "k3")), parse(body));
  }

  @Test
  void parse_malformedThrows() {
    assertThrows(IllegalArgumentException.class, () -> parse("not json"));
  }

  private static List<S3ObjectRef> parse(String body) {
    return S3EventNotificationParser.parse(body);
  }

  private static String quote(String s) {
    return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }
}
