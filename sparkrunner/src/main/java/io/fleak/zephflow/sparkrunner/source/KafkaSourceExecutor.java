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
package io.fleak.zephflow.sparkrunner.source;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.kafkasource.KafkaSourceCommand;
import io.fleak.zephflow.lib.commands.kafkasource.KafkaSourceDto;
import io.fleak.zephflow.lib.commands.source.SimpleSourceCommand;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import io.fleak.zephflow.sparkrunner.SparkDataConverter;
import io.fleak.zephflow.sparkrunner.SparkSchemas;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.unsafe.types.VariantVal;
import scala.collection.JavaConverters;

/**
 * Native Spark executor for Kafka sources.
 *
 * <p>Uses Spark's native Kafka integration to read from Kafka topics. Maps KafkaSourceCommand
 * configuration to Spark's readStream API options.
 */
@Slf4j
public class KafkaSourceExecutor implements SparkSourceExecutor {

  @Override
  public Dataset<Row> execute(
      SimpleSourceCommand<SerializedEvent> sourceCommand, SparkSession spark) {
    if (!(sourceCommand instanceof KafkaSourceCommand)) {
      throw new IllegalArgumentException(
          "Expected KafkaSourceCommand, got: " + sourceCommand.getClass());
    }

    KafkaSourceDto.Config kafkaConfig = (KafkaSourceDto.Config) sourceCommand.getCommandConfig();
    if (kafkaConfig == null) {
      throw new IllegalStateException("Kafka config not initialized");
    }
    log.info(
        "Executing Kafka source with native Spark integration: topic={}, broker={}",
        kafkaConfig.getTopic(),
        kafkaConfig.getBroker());

    Dataset<Row> kafkaStream =
        spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaConfig.getBroker())
            .option("subscribe", kafkaConfig.getTopic())
            .option("startingOffsets", mapOffsetReset(kafkaConfig))
            .options(buildKafkaOptions(kafkaConfig))
            .load();

    // Extract encoding type to avoid capturing the entire config in the lambda
    EncodingType encodingType = kafkaConfig.getEncodingType();

    MapPartitionsFunction<Row, Row> partitionProcessor =
        iterator -> {
          // 1. Create the deserializer ONCE for this partition
          FleakDeserializer<?> deserializer =
              DeserializerFactory.createDeserializerFactory(encodingType).createDeserializer();

          return new Iterator<>() {
            private final Queue<Row> outputBuffer = new LinkedList<>();

            @Override
            public boolean hasNext() {
              if (!outputBuffer.isEmpty()) {
                return true;
              }

              while (iterator.hasNext()) {
                Row row = iterator.next();
                byte[] valueBytes = row.getAs("value");
                byte[] keyBytes = row.getAs("key");
                SerializedEvent serializedEvent =
                    new SerializedEvent(keyBytes, valueBytes, Map.of());
                List<RecordFleakData> records;
                try {
                  records = deserializer.deserialize(serializedEvent);
                } catch (Exception e) {
                  String topic = row.getAs("topic");
                  int partition = row.getAs("partition");
                  long offset = row.getAs("offset");
                  log.warn(
                      "Failed to deserialize Kafka message. Skipping record."
                          + " Topic: {}, Partition: {}, Offset: {}",
                      topic,
                      partition,
                      offset,
                      e);
                  continue;
                }
                for (RecordFleakData record : records) {
                  Map<String, VariantVal> dataMap = SparkDataConverter.recordToMap(record);
                  outputBuffer.add(
                      RowFactory.create(JavaConverters.mapAsScalaMapConverter(dataMap).asScala()));
                }
                if (!outputBuffer.isEmpty()) {
                  return true;
                }
              }
              return false;
            }

            @Override
            public Row next() {
              // Standard iterator pattern: hasNext() must be called first
              if (!hasNext()) {
                throw new java.util.NoSuchElementException();
              }
              return outputBuffer.poll();
            }
          };
        };

    return kafkaStream
        .mapPartitions(partitionProcessor, Encoders.row(SparkSchemas.INPUT_EVENT_SCHEMA))
        .toDF();
  }

  private Map<String, String> buildKafkaOptions(KafkaSourceDto.Config config) {
    Map<String, String> options = new HashMap<>();

    options.put("kafka.session.timeout.ms", "10000");
    options.put("kafka.max.partition.fetch.bytes", "10485760");
    if (config.getProperties() != null) {
      options.putAll(config.getProperties());
    }

    return options;
  }

  private String mapOffsetReset(KafkaSourceDto.Config config) {
    if (config.getProperties() != null && config.getProperties().containsKey("auto.offset.reset")) {
      String offsetReset = config.getProperties().get("auto.offset.reset");
      return offsetReset.equals("earliest") ? "earliest" : "latest";
    }
    return "earliest";
  }
}
