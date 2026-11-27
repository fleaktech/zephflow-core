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
package io.fleak.zephflow.lib.kafka.clientfactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.fleak.zephflow.lib.kafka.KafkaConnectionValidator;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;

class KafkaConnectionValidatorClientFactoryTest {

  @Test
  void aCallerOutsideThePackageCanSupplyTheClientFactory() {
    Admin admin = mock(Admin.class);
    DescribeClusterResult result = mock(DescribeClusterResult.class);
    when(admin.describeCluster(any())).thenReturn(result);
    when(result.nodes())
        .thenReturn(KafkaFuture.completedFuture(List.of(new Node(1, "broker", 9092))));
    List<String> bootstrapServers = new ArrayList<>();
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "broker:9092");

    KafkaConnectionValidator.Status status =
        new KafkaConnectionValidator(
                actual -> {
                  bootstrapServers.add(actual.getProperty("bootstrap.servers"));
                  return admin;
                })
            .validate(properties, Duration.ofSeconds(1));

    assertEquals(
        List.of(KafkaConnectionValidator.Status.SUCCESS, List.of("broker:9092")),
        List.of(status, bootstrapServers));
  }
}
