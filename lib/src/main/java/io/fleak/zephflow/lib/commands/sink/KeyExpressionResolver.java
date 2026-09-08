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
package io.fleak.zephflow.lib.commands.sink;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves a configured routing-key expression (partition key, ordering key, FIFO message group id)
 * to a string, accepting any scalar value so a numeric or boolean field works as a key.
 *
 * <p>When the expression resolves to nothing — a missing field, or a record/array, which cannot be
 * a key — the record has no key and the sink falls back to its unkeyed behavior. That fallback
 * costs the per-key ordering guarantee the expression was configured for, so it is logged once per
 * resolver instance, with the offending record, rather than silently. Once per instance keeps a
 * wholly mistyped key from flooding the log, at the cost of reporting an intermittently missing key
 * only the first time.
 */
@Slf4j
public class KeyExpressionResolver {

  private static final int RECORD_SAMPLE_MAX_LENGTH = 512;

  private final PathExpression expression; // null when the key is not configured
  private final String configFieldName;
  private final String fallbackDescription;
  private final AtomicBoolean warned = new AtomicBoolean(false);

  /**
   * @param expression the configured key expression, or {@code null} when no key is configured
   * @param configFieldName the config property the expression came from, named in the warning
   * @param fallbackDescription what the sink does with an unkeyed record, named in the warning
   */
  public KeyExpressionResolver(
      @Nullable PathExpression expression, String configFieldName, String fallbackDescription) {
    this.expression = expression;
    this.configFieldName = configFieldName;
    this.fallbackDescription = fallbackDescription;
  }

  /** Returns the resolved key, or {@code null} when unconfigured or unresolvable. */
  @Nullable
  public String resolve(FleakData event) {
    if (expression == null) {
      return null;
    }
    String key = expression.getScalarStringValueFromEventOrDefault(event, null);
    if (key == null && warned.compareAndSet(false, true)) {
      log.warn(
          "{} '{}' did not resolve to a scalar (string/number/boolean) value: {}."
              + " First offending record: {}. Further occurrences are not logged.",
          configFieldName,
          expression,
          fallbackDescription,
          recordSample(event));
    }
    return key;
  }

  private static String recordSample(FleakData event) {
    String json = toJsonString(event);
    if (json == null || json.length() <= RECORD_SAMPLE_MAX_LENGTH) {
      return json;
    }
    return json.substring(0, RECORD_SAMPLE_MAX_LENGTH) + "...(truncated)";
  }
}
