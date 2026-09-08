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
package io.fleak.zephflow.lib.kafka;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.kafkasink.KafkaSinkDto;
import io.fleak.zephflow.lib.commands.kafkasource.KafkaSourceDto;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KafkaClientPropertiesTest {
  @Test
  void sourceUserPropertiesOverrideEveryDefault() {
    var properties =
        KafkaClientProperties.source(
            KafkaSourceDto.Config.builder()
                .broker("original:9092")
                .groupId("group")
                .properties(
                    Map.of(
                        "bootstrap.servers",
                        "effective:9092",
                        "enable.auto.commit",
                        "true",
                        "max.poll.records",
                        "17"))
                .build());
    assertEquals("effective:9092", properties.getProperty("bootstrap.servers"));
    assertEquals("true", properties.getProperty("enable.auto.commit"));
    assertEquals("17", properties.getProperty("max.poll.records"));
  }

  @Test
  void sinkPreservesModeUserAndCredentialPrecedence() {
    var context = JobContext.builder().build();
    context
        .getOtherProperties()
        .put(
            "credential",
            new java.util.HashMap<>(
                Map.of("username", "actual-user", "password", "actual-password")));
    var config =
        KafkaSinkDto.Config.builder()
            .broker("original:9092")
            .topic("topic")
            .encodingType("JSON_OBJECT")
            .storeAndForwardEnabled(true)
            .credentialId("credential")
            .securityProtocol("SASL_SSL")
            .saslMechanism("PLAIN")
            .properties(
                Map.of(
                    "bootstrap.servers",
                    "effective:9092",
                    "max.block.ms",
                    "123",
                    "sasl.jaas.config",
                    "ignored",
                    "security.protocol",
                    "PLAINTEXT",
                    "acks",
                    "1"))
            .build();
    var properties = KafkaClientProperties.sink(config, context);
    assertEquals("effective:9092", properties.getProperty("bootstrap.servers"));
    assertEquals("123", properties.getProperty("max.block.ms"));
    assertEquals("SASL_SSL", properties.getProperty("security.protocol"));
    assertTrue(properties.getProperty("sasl.jaas.config").contains("actual-password"));
    assertFalse(properties.containsKey("enable.idempotence"));
    assertEquals("2000", properties.getProperty("request.timeout.ms"));
  }
}
