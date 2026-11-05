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
        {"Records":[{"eventName":"ObjectCreated:Put","awsRegion":"us-east-1",
          "s3":{"bucket":{"name":"my-bucket"},"object":{"key":"path/to/file.json","size":42}}}]}
        """;
    assertEquals(
        List.of(new S3ObjectRef("my-bucket", "path/to/file.json", "us-east-1")), parse(body));
  }

  @Test
  void parse_snsWrappedNotification() {
    String inner =
        "{\"Records\":[{\"eventName\":\"ObjectCreated:Post\",\"awsRegion\":\"us-east-1\","
            + "\"s3\":{\"bucket\":{\"name\":\"b\"},\"object\":{\"key\":\"k.csv\"}}}]}";
    String body =
        "{\"Type\":\"Notification\",\"TopicArn\":\"arn:aws:sns:us-east-1:123:topic\","
            + "\"Message\":"
            + quote(inner)
            + "}";
    assertEquals(List.of(new S3ObjectRef("b", "k.csv", "us-east-1")), parse(body));
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
          {"eventName":"ObjectCreated:Put","awsRegion":"us-east-1","s3":{"bucket":{"name":"b"},"object":{"key":"a+b.json"}}},
          {"eventName":"ObjectCreated:Put","awsRegion":"us-east-1","s3":{"bucket":{"name":"b"},"object":{"key":"dir/with%20space.json"}}},
          {"eventName":"ObjectCreated:Put","awsRegion":"us-east-1","s3":{"bucket":{"name":"b"},"object":{"key":"100%25.json"}}}
        ]}
        """;
    assertEquals(
        List.of(
            new S3ObjectRef("b", "a b.json", "us-east-1"),
            new S3ObjectRef("b", "dir/with space.json", "us-east-1"),
            new S3ObjectRef("b", "100%.json", "us-east-1")),
        parse(body));
  }

  @Test
  void parse_multipleRecordsMixedEvents() {
    String body =
        """
        {"Records":[
          {"eventName":"ObjectCreated:Put","awsRegion":"us-east-1","s3":{"bucket":{"name":"b1"},"object":{"key":"k1"}}},
          {"eventName":"ObjectRemoved:Delete","s3":{"bucket":{"name":"b1"},"object":{"key":"k2"}}},
          {"eventName":"ObjectCreated:CompleteMultipartUpload","awsRegion":"us-east-1","s3":{"bucket":{"name":"b2"},"object":{"key":"k3"}}}
        ]}
        """;
    assertEquals(
        List.of(new S3ObjectRef("b1", "k1", "us-east-1"), new S3ObjectRef("b2", "k3", "us-east-1")),
        parse(body));
  }

  @Test
  void parse_malformedThrows() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> parse("not json"));
    assertFalse(e instanceof S3EventNotificationParser.NotS3NotificationException);
  }

  @Test
  void parse_snsWrappedTestEventReturnsEmpty() {
    String inner = "{\"Service\":\"Amazon S3\",\"Event\":\"s3:TestEvent\",\"Bucket\":\"b\"}";
    assertEquals(List.of(), parse("{\"Type\":\"Notification\",\"Message\":" + quote(inner) + "}"));
  }

  @Test
  void parse_snsSubscriptionHandshakeReturnsEmpty() {
    for (String type : List.of("SubscriptionConfirmation", "UnsubscribeConfirmation")) {
      String body =
          "{\"Type\":\""
              + type
              + "\",\"TopicArn\":\"arn:aws:sns:us-east-1:123:topic\",\"Message\":\"You have chosen"
              + " to subscribe\",\"SubscribeURL\":\"https://sns.us-east-1.amazonaws.com/?Action=x\"}";
      assertEquals(List.of(), parse(body), type);
    }
  }

  @Test
  void parse_eventBridgeS3EventIsNotS3() {
    assertThrows(
        S3EventNotificationParser.NotS3NotificationException.class,
        () ->
            parse(
                "{\"version\":\"0\",\"source\":\"aws.s3\",\"detail-type\":\"Object Created\","
                    + "\"detail\":{\"bucket\":{\"name\":\"b\"},\"object\":{\"key\":\"k\"}}}"));
  }

  @Test
  void parse_ordinaryJsonEventIsNotS3() {
    S3EventNotificationParser.NotS3NotificationException e =
        assertThrows(
            S3EventNotificationParser.NotS3NotificationException.class,
            () -> parse("{\"event_id\":1,\"user\":\"alice\",\"action\":\"login\"}"));
    assertEquals("not an S3 event notification: no \"Records\" array", e.getMessage());
  }

  @Test
  void parse_nonObjectJsonIsNotS3() {
    for (String body : List.of("[1,2]", "42", "\"text\"", "null")) {
      assertThrows(
          S3EventNotificationParser.NotS3NotificationException.class, () -> parse(body), body);
    }
  }

  @Test
  void parse_recordsWithoutS3ObjectAreNotS3() {
    S3EventNotificationParser.NotS3NotificationException e =
        assertThrows(
            S3EventNotificationParser.NotS3NotificationException.class,
            () -> parse("{\"Records\":[{\"id\":1},{\"id\":2}]}"));
    assertEquals("not an S3 event notification: a record has no \"s3\" object", e.getMessage());
  }

  @Test
  void parse_snsWrappedOrdinaryJsonIsNotS3() {
    assertThrows(
        S3EventNotificationParser.NotS3NotificationException.class,
        () -> parse("{\"Type\":\"Notification\",\"Message\":" + quote("{\"a\":1}") + "}"));
  }

  @Test
  void preservesRegionForEveryRecordInDirectAndSnsMessages() {
    String inner =
        """
        {"Records":[
          {"eventName":"ObjectCreated:Put","awsRegion":"eu-central-1","s3":{"bucket":{"name":"b"},"object":{"key":"one"}}},
          {"eventName":"ObjectCreated:Post","awsRegion":"us-west-2","s3":{"bucket":{"name":"b"},"object":{"key":"two"}}},
          {"eventName":"ObjectCreated:CompleteMultipartUpload","awsRegion":"eu-central-1","s3":{"bucket":{"name":"b"},"object":{"key":"three"}}}
        ]}
        """;
    List<S3ObjectRef> expected =
        List.of(
            new S3ObjectRef("b", "one", "eu-central-1"),
            new S3ObjectRef("b", "two", "us-west-2"),
            new S3ObjectRef("b", "three", "eu-central-1"));
    assertEquals(expected, parse(inner));
    assertEquals(
        expected,
        parse("{\"Type\":\"Notification\",\"Message\":" + quote(inner.replace("\n", "")) + "}"));
  }

  @Test
  void invalidRegionStaysOnItsObjectWithoutDiscardingValidSibling() {
    for (String region :
        List.of(
            "",
            ",\"awsRegion\":null",
            ",\"awsRegion\":42",
            ",\"awsRegion\":{}",
            ",\"awsRegion\":[]")) {
      String body =
          """
          {"Records":[
            {"eventName":"ObjectCreated:Put"%s,"s3":{"bucket":{"name":"b"},"object":{"key":"bad"}}},
            {"eventName":"ObjectCreated:Put","awsRegion":"us-west-2","s3":{"bucket":{"name":"b"},"object":{"key":"good"}}}
          ]}
          """
              .formatted(region);
      assertEquals(
          List.of(new S3ObjectRef("b", "bad", null), new S3ObjectRef("b", "good", "us-west-2")),
          parse(body));
    }
  }

  private static List<S3ObjectRef> parse(String body) {
    return S3EventNotificationParser.parse(body);
  }

  private static String quote(String s) {
    return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }
}
