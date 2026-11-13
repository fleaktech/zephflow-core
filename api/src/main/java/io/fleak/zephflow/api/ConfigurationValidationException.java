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
package io.fleak.zephflow.api;

import com.google.common.base.Strings;
import java.util.List;
import java.util.Objects;

/**
 * Adds actionable details to an existing configuration rejection. Graph validation owns severity;
 * consumers retaining an existing exception representation can use {@link #originalFailure()}.
 */
public final class ConfigurationValidationException extends IllegalArgumentException {
  private final RuntimeException originalFailure;
  private final List<ConfigurationIssue> issues;

  /**
   * Preserves the original message, exception type, and cause chain alongside public diagnostics.
   */
  public ConfigurationValidationException(
      RuntimeException originalFailure, List<ConfigurationIssue> issues) {
    super(Objects.requireNonNull(originalFailure, "originalFailure").getMessage(), originalFailure);
    if (originalFailure instanceof ConfigurationValidationException)
      throw new IllegalArgumentException(
          "Preserve the original failure when remapping diagnostics");
    this.originalFailure = originalFailure;
    this.issues = List.copyOf(issues);
    if (this.issues.isEmpty())
      throw new IllegalArgumentException("Configuration issues are required");
  }

  /** Attaches a single known field and safe message to an existing failure. */
  public static ConfigurationValidationException at(
      ConfigurationPath path, RuntimeException originalFailure, String message) {
    return new ConfigurationValidationException(
        originalFailure, List.of(new ConfigurationIssue(path, message)));
  }

  /** Enriches an existing argument check whose message is already safe to show publicly. */
  public static void require(boolean condition, ConfigurationPath path, String message) {
    require(condition, path, message, message);
  }

  /** Adds diagnostics to an existing null check, retaining its original exception and message. */
  public static void requireNonNull(
      Object value, ConfigurationPath path, String message, String originalMessage) {
    try {
      Objects.requireNonNull(value, originalMessage);
    } catch (NullPointerException failure) {
      throw at(path, failure, message);
    }
  }

  /**
   * Enriches an existing check without changing its predicate or legacy Guava message formatting.
   * Rejected values may occur in the legacy message, but must not occur in the public message.
   */
  public static void require(
      boolean condition,
      ConfigurationPath path,
      String message,
      String originalMessage,
      Object... arguments) {
    if (!condition) {
      throw at(
          path,
          new IllegalArgumentException(Strings.lenientFormat(originalMessage, arguments)),
          message);
    }
  }

  /** The failure used by existing consumers that expose its concrete class name or cause. */
  public RuntimeException originalFailure() {
    return originalFailure;
  }

  /** The recognized problems; this does not promise aggregation of all possible errors. */
  public List<ConfigurationIssue> issues() {
    return issues;
  }

  /** Prefixes delegated diagnostics while keeping the original failure unchanged. */
  public ConfigurationValidationException withPrefix(ConfigurationPath prefix) {
    return new ConfigurationValidationException(
        originalFailure, issues.stream().map(issue -> issue.withPrefix(prefix)).toList());
  }
}
