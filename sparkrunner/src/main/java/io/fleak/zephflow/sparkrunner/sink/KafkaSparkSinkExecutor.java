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
package io.fleak.zephflow.sparkrunner.sink;

import static org.apache.spark.sql.functions.*;

import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.lib.commands.kafkasink.KafkaSinkCommand;
import io.fleak.zephflow.lib.commands.kafkasink.KafkaSinkDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Spark-native Kafka sink executor.
 *
 * <p>Uses Spark's built-in Kafka sink for better performance and scalability.
 */
@Slf4j
public class KafkaSparkSinkExecutor implements SparkSinkExecutor {

  @Override
  public void execute(Dataset<Row> data, OperatorCommand sinkCommand, SparkSession spark) {
    if (!(sinkCommand instanceof KafkaSinkCommand kafkaCmd)) {
      throw new IllegalArgumentException(
          "Expected KafkaSinkCommand but got: " + sinkCommand.getClass().getName());
    }

    KafkaSinkDto.Config config = (KafkaSinkDto.Config) kafkaCmd.getCommandConfig();

    log.info(
        "Writing to Kafka topic '{}' at broker '{}' using Spark native sink",
        config.getTopic(),
        config.getBroker());

    // Transform OUTPUT_EVENT_SCHEMA (nodeId, data) to Kafka format (key, value)
    // For now, we'll use null key and JSON-encoded value
    Dataset<Row> kafkaFormat =
        data.select(
            lit(null).cast("binary").as("key"), // No partition key for now
            to_json(col("data")).cast("binary").as("value"));

    // Write to Kafka using Spark's native sink
    kafkaFormat
        .write()
        .format("kafka")
        .option("kafka.bootstrap.servers", config.getBroker())
        .option("topic", config.getTopic())
        .save();
  }
}
