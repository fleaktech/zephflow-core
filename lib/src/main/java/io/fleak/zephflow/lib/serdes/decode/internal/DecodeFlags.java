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
package io.fleak.zephflow.lib.serdes.decode.internal;

import lombok.Getter;

/**
 * Marks set by the layer where a failure happened. Classification looks only at these marks, never
 * at the exception that reaches the cursor, because parsers wrap or swallow exceptions.
 */
@Getter
public final class DecodeFlags {

  private Throwable readFailure;
  private Throwable decompressFailure;
  private BudgetExceeded budgetExceeded;

  public void markRead(Throwable cause) {
    if (readFailure == null) {
      readFailure = cause;
    }
  }

  public void markDecompress(Throwable cause) {
    if (decompressFailure == null) {
      decompressFailure = cause;
    }
  }

  public void markBudget(BudgetExceeded exceeded) {
    if (budgetExceeded == null) {
      budgetExceeded = exceeded;
    }
  }

  /** What the budget layer saw when the current record went over its limit. */
  public record BudgetExceeded(long recordStart, long sizeSoFar, byte[] preview) {}
}
