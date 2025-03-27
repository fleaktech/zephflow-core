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
package io.fleak.zephflow.lib.commands.kafkasource;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;

/** Test that connection failures cause exceptions and not hangs */
public class KafkaSourceConnectionFailureTest {

  @Test
  public void testExceptionThrown() throws InterruptedException {
    var consumerFactory = new KafkaConsumerClientFactory();
    var consumerProps = new Properties();
    consumerProps.put("key.deserializer", ByteArrayDeserializer.class.getName());
    consumerProps.put("value.deserializer", ByteArrayDeserializer.class.getName());
    consumerProps.put("group.id", "test");

    consumerProps.put("bootstrap.servers", "localhost:9091");
    var exception = new AtomicReference<Exception>();
    var monitor =
        consumerFactory.createAndStartHealthMonitor(
            consumerProps,
            500,
            (e) -> {
              System.out.println("Expected monitor exception: " + e);
              exception.set(e);
            });
    try {
      Thread.sleep(3000);
      assertNotNull(exception.get());
    } finally {
      monitor.stop();
    }
  }

  @Test
  public void testNoExceptionThrown() {
    // This demonstrates our base assumption that no exception is thrown when the consumer
    // cannot connect to a broker. The address only needs to be resolvable, but not have an
    // accessible kafka cluster.
    // The consumer will try to connect indefinitely.
    // If this is not true anymore we need to reevaluate our error handling.
    var consumerFactory = new KafkaConsumerClientFactory();
    var consumerProps = new Properties();
    consumerProps.put("bootstrap.servers", "localhost:9091");
    consumerProps.put("key.deserializer", ByteArrayDeserializer.class.getName());
    consumerProps.put("value.deserializer", ByteArrayDeserializer.class.getName());
    consumerProps.put("group.id", "test");
    var consumer = consumerFactory.createKafkaConsumer(consumerProps);
    consumer.subscribe(List.of("topic1"));
    consumer.poll(Duration.of(10, ChronoUnit.SECONDS));
    consumer.close();
  }
}
