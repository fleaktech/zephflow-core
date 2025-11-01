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
package io.fleak.zephflow.sparkrunner;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Defines Spark schemas for ZephFlow data structures.
 *
 * <p>RecordFleakData is represented as MapType(StringType, VariantType) where: - Keys are field
 * names from RecordFleakData.payload - Values are Variant types that can hold any FleakData (nested
 * records, arrays, primitives)
 *
 * <p>Created by bolei on 11/1/25
 */
public class SparkSchemas {

  /**
   * Schema for input events from source.
   *
   * <p>Structure: - data: Map[String, Variant] (represents RecordFleakData.payload)
   */
  public static final StructType INPUT_EVENT_SCHEMA =
      new StructType(
          new StructField[] {
            new StructField(
                "data",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.VariantType),
                false,
                Metadata.empty())
          });

  /**
   * Schema for processing output (from DagResult.outputEvents).
   *
   * <p>Structure: - nodeId: String (the node that produced this event) - data: Map[String, Variant]
   * (represents RecordFleakData.payload)
   */
  public static final StructType OUTPUT_EVENT_SCHEMA =
      new StructType(
          new StructField[] {
            new StructField("nodeId", DataTypes.StringType, false, Metadata.empty()),
            new StructField(
                "data",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.VariantType),
                false,
                Metadata.empty())
          });

  private SparkSchemas() {
    // Utility class, prevent instantiation
  }
}
