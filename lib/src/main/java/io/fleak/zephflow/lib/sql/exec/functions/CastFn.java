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
package io.fleak.zephflow.lib.sql.exec.functions;

import com.fasterxml.jackson.databind.JsonNode;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

public class CastFn extends BaseFunction {

  public static final String NAME = "typeCast";

  public CastFn(TypeSystem typeSystem) {
    super(typeSystem, NAME);
  }

  public Object apply(List<Object> args) {
    assertArgs(args, 2, "<cast string>, <object>");
    var type = args.get(0).toString().toLowerCase();
    var typeToCast = args.get(1);

    if (type.equals("long")
        || type.startsWith("int")
        || type.equals("integer")
        || type.equals("short")
        || type.equals("smallint")) {
      return typeSystem.lookupTypeCast(Long.class).cast(typeToCast);
    } else if (type.equals("double precision") || type.equals("float")) {
      return typeSystem.lookupTypeCast(Double.class).cast(typeToCast);
    } else if (type.equals("string") || type.equals("text") || type.startsWith("varchar")) {
      return typeSystem.lookupTypeCast(String.class).cast(typeToCast);
    } else if (type.equals("date")) {
      return typeSystem.lookupTypeCast(Date.class).cast(typeToCast);
    } else if (type.equals("timestamp")) {
      return typeSystem.lookupTypeCast(Timestamp.class).cast(typeToCast);
    } else if (type.equals("bigint") || type.equals("numeric")) {
      return typeSystem.lookupTypeCast(BigDecimal.class).cast(typeToCast);
    } else if (type.startsWith("bool")) {
      return typeSystem.lookupTypeCast(Boolean.class).cast(typeToCast);
    } else if (type.equals("json")) {
      return typeSystem.lookupTypeCast(JsonNode.class).cast(typeToCast);
    }

    throw new RuntimeException("type cast " + type + " is not supported");
  }
}
