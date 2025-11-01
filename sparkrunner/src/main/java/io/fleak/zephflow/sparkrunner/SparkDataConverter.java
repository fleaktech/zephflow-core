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

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.utils.JsonUtils;
import io.fleak.zephflow.runner.DagResult;
import java.time.ZoneOffset;
import java.util.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.types.variant.VariantBuilder;
import org.apache.spark.unsafe.types.VariantVal;
import scala.collection.JavaConverters;

/**
 * Converts between ZephFlow's RecordFleakData and Spark's Row format.
 *
 * <p>Handles bidirectional conversion: - RecordFleakData ↔ Map&lt;String, Variant&gt; -
 * DagResult.outputEvents → List&lt;Row&gt; - List&lt;Row&gt; → List&lt;RecordFleakData&gt;
 *
 * <p>Uses Spark's Variant type to handle dynamic/nested FleakData structures.
 *
 * <p>Created by bolei on 11/1/25
 */
public class SparkDataConverter {

  /**
   * Converts DagResult.outputEvents to Spark Rows.
   *
   * <p>Flattens Map&lt;nodeId, List&lt;RecordFleakData&gt;&gt; into List&lt;Row(nodeId, data)&gt;.
   *
   * @param dagResult The DAG execution result
   * @return List of Rows with schema: OUTPUT_EVENT_SCHEMA
   */
  public static List<Row> dagResultToRows(DagResult dagResult) {
    List<Row> rows = new ArrayList<>();

    for (Map.Entry<String, List<RecordFleakData>> entry : dagResult.getOutputEvents().entrySet()) {
      String nodeId = entry.getKey();
      List<RecordFleakData> events = entry.getValue();

      for (RecordFleakData event : events) {
        Map<String, VariantVal> dataMap = recordToMap(event);
        Row row = RowFactory.create(nodeId, dataMap);
        rows.add(row);
      }
    }

    return rows;
  }

  /**
   * Converts RecordFleakData to Map&lt;String, VariantVal&gt;.
   *
   * <p>Maps RecordFleakData.payload → Map&lt;String, VariantVal&gt; where each FleakData value is
   * converted to VariantVal (Spark's internal Variant representation).
   *
   * @param record The RecordFleakData to convert
   * @return Map of field names to VariantVal values
   */
  public static Map<String, VariantVal> recordToMap(RecordFleakData record) {
    Map<String, VariantVal> result = new HashMap<>();

    for (Map.Entry<String, FleakData> entry : record.getPayload().entrySet()) {
      String key = entry.getKey();
      FleakData value = entry.getValue();
      Variant variant = fleakDataToVariant(value);
      // Convert Variant to VariantVal (Spark's internal representation)
      VariantVal variantVal = new VariantVal(variant.getValue(), variant.getMetadata());
      result.put(key, variantVal);
    }

    return result;
  }

  /**
   * Converts FleakData to Spark Variant.
   *
   * <p>Handles all FleakData types: - RecordFleakData → Variant object (recursive) - ArrayFleakData
   * → Variant array - Primitives → Variant primitives (string, number, boolean)
   *
   * @param data The FleakData to convert
   * @return Variant representation
   */
  public static Variant fleakDataToVariant(FleakData data) {
    try {
      if (data == null) {
        return VariantBuilder.parseJson("null", false);
      }

      // Get the unwrapped Java object representation
      Object unwrapped = data.unwrap();

      // Convert to JSON string using JsonUtils, then parse as Variant
      // This handles all nested structures correctly
      String json = JsonUtils.toJsonString(unwrapped);
      return VariantBuilder.parseJson(json, false);
    } catch (Exception e) {
      throw new RuntimeException("Failed to convert FleakData to Variant", e);
    }
  }

  /**
   * Converts INPUT_EVENT_SCHEMA Row to RecordFleakData.
   *
   * <p>Schema: [data: Map&lt;String, VariantVal&gt;]
   *
   * @param row Row with INPUT_EVENT_SCHEMA
   * @return RecordFleakData
   */
  public static RecordFleakData inputRowToRecord(Row row) {
    // Spark returns Scala Map, need to convert to Java Map
    scala.collection.Map<String, VariantVal> scalaMap = row.getAs("data");
    Map<String, VariantVal> dataMap = JavaConverters.mapAsJavaMapConverter(scalaMap).asJava();
    return mapToRecord(dataMap);
  }

  /**
   * Converts Map&lt;String, VariantVal&gt; to RecordFleakData.
   *
   * @param dataMap The map to convert
   * @return RecordFleakData
   */
  public static RecordFleakData mapToRecord(Map<String, VariantVal> dataMap) {
    Map<String, FleakData> payload = new HashMap<>();

    for (Map.Entry<String, VariantVal> entry : dataMap.entrySet()) {
      String key = entry.getKey();
      VariantVal variantVal = entry.getValue();
      // Convert VariantVal to Variant for processing
      Variant variant = new Variant(variantVal.getValue(), variantVal.getMetadata());
      FleakData fleakData = variantToFleakData(variant);
      payload.put(key, fleakData);
    }

    return new RecordFleakData(payload);
  }

  /**
   * Converts Spark Variant to FleakData.
   *
   * <p>Reverse of fleakDataToVariant. Uses Variant's JSON representation to reconstruct FleakData.
   *
   * @param variant The Variant to convert
   * @return FleakData
   */
  public static FleakData variantToFleakData(Variant variant) {
    if (variant == null) {
      return null;
    }

    // Convert Variant to JSON string using toJson() method with UTC timezone
    String json = variant.toJson(ZoneOffset.UTC);

    // Parse JSON string to Java object using JsonUtils
    Object obj = JsonUtils.fromJsonString(json, Object.class);

    // Use FleakData.wrap to create appropriate FleakData type
    return FleakData.wrap(obj);
  }

  private SparkDataConverter() {
    // Utility class, prevent instantiation
  }
}
