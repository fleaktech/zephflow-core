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
package io.fleak.zephflow.api.execution;

/** An observed delivery receipt. Null counts mean the adapter cannot establish that count. */
public record EffectOutcome(
    Long attemptedCount,
    Long acknowledgedCount,
    Long definiteFailureCount,
    Long unknownCount,
    Long notAttemptedCount,
    String acknowledgementKind,
    Delivery delivery) {
  public enum Delivery {
    NOT_ATTEMPTED,
    ACKNOWLEDGED,
    PARTIAL,
    FAILED,
    UNKNOWN
  }

  public static EffectOutcome unknown(long attemptedCount) {
    return new EffectOutcome(attemptedCount, 0L, 0L, attemptedCount, 0L, "none", Delivery.UNKNOWN);
  }

  public static EffectOutcome opaque(boolean returnedSuccessfully) {
    return new EffectOutcome(
        null,
        null,
        null,
        null,
        null,
        returnedSuccessfully ? "operation_returned" : "none",
        returnedSuccessfully ? Delivery.ACKNOWLEDGED : Delivery.UNKNOWN);
  }

  /** Classifies known counts; locally rejected records are also not attempted remotely. */
  public static EffectOutcome counted(
      long attempted,
      long acknowledged,
      long failed,
      long unknown,
      long notAttempted,
      String acknowledgementKind) {
    Delivery delivery =
        acknowledged > 0
            ? failed > 0 || unknown > 0 || notAttempted > 0
                ? Delivery.PARTIAL
                : Delivery.ACKNOWLEDGED
            : unknown > 0
                ? Delivery.UNKNOWN
                : failed > 0
                    ? Delivery.FAILED
                    : notAttempted > 0 ? Delivery.NOT_ATTEMPTED : Delivery.ACKNOWLEDGED;
    return new EffectOutcome(
        attempted, acknowledged, failed, unknown, notAttempted, acknowledgementKind, delivery);
  }

  /** Combines disjoint batches without inventing counts absent from either receipt. */
  public EffectOutcome merge(EffectOutcome other) {
    Long acknowledged = sum(acknowledgedCount, other.acknowledgedCount);
    Long failed = sum(definiteFailureCount, other.definiteFailureCount);
    Long unknown = sum(unknownCount, other.unknownCount);
    Long notAttempted = sum(notAttemptedCount, other.notAttemptedCount);
    Delivery combined =
        delivery == other.delivery
            ? delivery
            : acknowledged != null && acknowledged > 0
                ? Delivery.PARTIAL
                : unknown != null && unknown > 0
                    ? Delivery.UNKNOWN
                    : failed != null && failed > 0
                        ? Delivery.FAILED
                        : notAttempted != null && notAttempted > 0
                            ? Delivery.NOT_ATTEMPTED
                            : Delivery.UNKNOWN;
    return new EffectOutcome(
        sum(attemptedCount, other.attemptedCount),
        acknowledged,
        failed,
        unknown,
        notAttempted,
        acknowledgementKind.equals(other.acknowledgementKind) ? acknowledgementKind : "mixed",
        combined);
  }

  private static Long sum(Long left, Long right) {
    return left == null || right == null ? null : Math.addExact(left, right);
  }
}
