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
package io.fleak.zephflow.lib.sql.exec.functions.strings;

import io.fleak.zephflow.lib.sql.exec.functions.BaseFunction;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import java.util.List;
import org.apache.commons.collections4.map.LRUMap;
import org.opensearch.grok.*;

public class GrokFn extends BaseFunction {

  public static final String NAME = "grok";

  public final LRUMap<String, Grok> grokCache = new LRUMap<>(100);

  public GrokFn(TypeSystem typeSystem) {
    super(typeSystem, NAME);
  }

  @Override
  public Object apply(List<Object> args) {

    assertArgs(args, 2, "(pattern, input)");

    var pattern = args.get(0);
    if (pattern == null) return null;

    var input = args.get(1);
    if (input == null) return null;

    var patternKey = pattern.toString();
    var grok = grokCache.get(patternKey);

    if (grok == null) {
      grok = new Grok(Grok.BUILTIN_PATTERNS, patternKey, (v) -> outputWarnings(v));
      grokCache.put(pattern.toString(), grok);
    }

    var m = grok.captures(input.toString());
    return m;
  }

  private void outputWarnings(String v) {
    System.out.println(v);
  }
}
