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
package io.fleak.zephflow.lib.pathselect;

import io.fleak.zephflow.api.structure.FleakData;
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
 * resolver instance rather than silently.
 */
@Slf4j
public class KeyExpressionResolver {

  private final PathExpression expression; // null when the key is not configured
  private final String configFieldName;
  private final String fallbackDescription;
  private final AtomicBoolean warned = new AtomicBoolean(false);

  private KeyExpressionResolver(
      @Nullable PathExpression expression, String configFieldName, String fallbackDescription) {
    this.expression = expression;
    this.configFieldName = configFieldName;
    this.fallbackDescription = fallbackDescription;
  }

  /**
   * @param expression the configured key expression, or {@code null} when no key is configured
   * @param configFieldName the config property the expression came from, named in the warning
   * @param fallbackDescription what the sink does with an unkeyed record, named in the warning
   */
  public static KeyExpressionResolver of(
      @Nullable PathExpression expression, String configFieldName, String fallbackDescription) {
    return new KeyExpressionResolver(expression, configFieldName, fallbackDescription);
  }

  public boolean isConfigured() {
    return expression != null;
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
          "{} '{}' did not resolve to a scalar (string/number/boolean) value for a record: {}."
              + " Further occurrences are not logged.",
          configFieldName,
          expression,
          fallbackDescription);
    }
    return key;
  }
}
