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

import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import java.sql.Timestamp;
import java.util.List;

public class DateTruncFn extends BaseFunction {

  public static final String NAME = "datetrunct";

  public DateTruncFn(TypeSystem typeSystem) {
    super(typeSystem, NAME);
  }

  @SuppressWarnings("all")
  public Object apply(List<Object> args) {
    assertArgs(args, 2, "<date field>, <date>");
    var datePart = args.get(0).toString();
    var arg = args.get(1);
    var dateObj = typeSystem.lookupTypeCast(Timestamp.class).cast(arg);
    /** sets to the "zero" value a date part */
    return null;
  }
}
