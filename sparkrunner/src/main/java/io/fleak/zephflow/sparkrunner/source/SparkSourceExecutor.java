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

import io.fleak.zephflow.lib.commands.source.SimpleSourceCommand;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Strategy interface for executing source commands in Spark.
 *
 * <p>Implementations can either use Spark-native sources (Kafka, Delta) or fall back to existing
 * ZephFlow SourceCommand implementations.
 */
public interface SparkSourceExecutor {

  /**
   * Execute the source command and return a Dataset with INPUT_EVENT_SCHEMA.
   *
   * @param sourceCommand The source command to execute
   * @param spark The Spark session
   * @return Dataset with schema: Map<String, VariantVal>
   * @throws Exception if source execution fails
   */
  Dataset<Row> execute(SimpleSourceCommand<SerializedEvent> sourceCommand, SparkSession spark)
      throws Exception;
}
