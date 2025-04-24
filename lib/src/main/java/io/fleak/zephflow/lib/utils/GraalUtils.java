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
package io.fleak.zephflow.lib.utils;

import io.fleak.zephflow.api.structure.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.graalvm.polyglot.Value;

/** Created by bolei on 4/22/25 */
public interface GraalUtils {
  static FleakData graalValueToFleakData(Value value) {
    if (value == null || value.isNull()) {
      return null; // Represent FEEL null
    } else if (value.isString()) {
      return new StringPrimitiveFleakData(value.asString());
    } else if (value.isBoolean()) {
      return new BooleanPrimitiveFleakData(value.asBoolean());
    } else if (value.isNumber()) {
      // Prioritize Long if it fits, otherwise Double
      if (value.fitsInLong()) {
        return new NumberPrimitiveFleakData(
            value.asLong(), NumberPrimitiveFleakData.NumberType.LONG);
      } else if (value.fitsInDouble()) {
        return new NumberPrimitiveFleakData(
            value.asDouble(), NumberPrimitiveFleakData.NumberType.DOUBLE);
      } else {
        // Handle BigInteger/BigDecimal if necessary and FleakData supports it
        // Fallback: represent as String or throw error? Let's try String for now.
        System.err.println(
            "Warning: Python returned a large number that doesn't fit Long/Double: "
                + value
                + ". Representing as String.");
        return new StringPrimitiveFleakData(value.toString());
        // Or: throw new IllegalArgumentException("Cannot represent Python number in FleakData: " +
        // value);
      }
    } else if (value.hasArrayElements()) {
      long size = value.getArraySize();
      List<FleakData> list = new ArrayList<>((int) size); // Preallocate list
      for (long i = 0; i < size; i++) {
        list.add(graalValueToFleakData(value.getArrayElement(i))); // Recursive conversion
      }
      return new ArrayFleakData(list);
    } else if (value.hasMembers()) { // Represents a Python dict or object with attributes
      Map<String, FleakData> map = new HashMap<>();
      // Use getMemberKeys() which returns a Set<String>
      for (String key : value.getMemberKeys()) {
        map.put(key, graalValueToFleakData(value.getMember(key))); // Recursive conversion
      }
      return new RecordFleakData(map);
    }
    // --- Host Object Handling (Optional but recommended) ---
    else if (value.isHostObject()) {
      // If Python returns one of the Java objects we passed in
      Object hostObject = value.asHostObject();
      // Attempt to re-wrap it if it's a known type, otherwise convert to string/error
      try {
        return FleakData.wrap(hostObject); // Assuming FleakData.wrap can handle the unwrapped types
      } catch (Exception e) {
        System.err.println(
            "Warning: Python returned a Java host object of unhandled type: "
                + hostObject.getClass().getName()
                + ". Converting to String.");
        return new StringPrimitiveFleakData(hostObject.toString());
      }
    }
    // --- Fallback for Unknown Types ---
    else {
      System.err.println(
          "Warning: Received unsupported type from Python: "
              + value.getMetaObject()
              + ". Converting to string representation.");
      return new StringPrimitiveFleakData(value.toString());
      // Or: throw new IllegalArgumentException("Cannot convert Python type to FleakData: " +
      // value.getMetaObject());
    }
  }
}
