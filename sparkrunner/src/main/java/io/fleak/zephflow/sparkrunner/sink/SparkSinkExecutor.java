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

import io.fleak.zephflow.api.OperatorCommand;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Strategy interface for executing sink commands in Spark.
 *
 * <p>Implementations can use Spark-native sinks (Kafka, Delta, S3) for better performance and
 * scalability.
 */
public interface SparkSinkExecutor {

  /**
   * Execute the sink command and write data to the destination.
   *
   * @param data Dataset with OUTPUT_EVENT_SCHEMA (nodeId, data)
   * @param sinkCommand The sink command configuration
   * @param spark The Spark session
   * @throws Exception if sink execution fails
   */
  void execute(Dataset<Row> data, OperatorCommand sinkCommand, SparkSession spark) throws Exception;
}
