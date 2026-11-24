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
package io.fleak.zephflow.lib.parser;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.parser.extractions.ExtractionRule;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.NonNull;

/** Compiled parser rules shared by ordinary execution and detailed inspection. */
public interface CompiledRules {

  enum TargetOutcome {
    EXTRACTED,
    NO_FIELDS_EXTRACTED,
    SKIPPED_MISSING,
    SKIPPED_NON_TEXT,
    SKIPPED_EMPTY
  }

  enum InputKind {
    MISSING,
    NULL,
    TEXT,
    EMPTY_TEXT,
    OBJECT,
    OTHER_NON_TEXT
  }

  /** One rule that actually ran, in execution order. rulePath is relative to its configuration. */
  record RuleEvent(
      List<String> targetFieldPath,
      InputKind inputKind,
      TargetOutcome targetOutcome,
      boolean removedTarget,
      Set<String> extractedKeys,
      List<String> rulePath) {
    public RuleEvent {
      targetFieldPath = List.copyOf(targetFieldPath);
      extractedKeys = Set.copyOf(extractedKeys);
      rulePath = List.copyOf(rulePath);
    }
  }

  /** The root rule's outcome and extraction, followed by the trace including any dispatch rules. */
  record ParseResult(
      RecordFleakData outputRecord,
      TargetOutcome targetOutcome,
      Set<String> extractedKeys,
      List<RuleEvent> events) {
    public ParseResult {
      extractedKeys = Set.copyOf(extractedKeys);
      events = List.copyOf(events);
    }
  }

  /** Keeps the original failure and identifies the rule that failed, without executing it again. */
  @Getter
  final class DetailedParseException extends RuntimeException {
    private final List<String> rulePath;
    private final List<String> targetFieldPath;
    private final InputKind inputKind;
    private final List<RuleEvent> events;

    private DetailedParseException(
        RuntimeException failure,
        List<String> rulePath,
        List<String> targetFieldPath,
        InputKind inputKind,
        List<RuleEvent> events) {
      super(failure.getMessage(), failure.getCause() == null ? failure : failure.getCause());
      this.rulePath = List.copyOf(rulePath);
      this.targetFieldPath = List.copyOf(targetFieldPath);
      this.inputKind = inputKind;
      this.events = List.copyOf(events);
    }
  }

  record ParseRule(
      @NonNull String targetField,
      boolean removeTargetField,
      @NonNull ExtractionRule extractionRule,
      DispatchRule dispatchRule,
      List<String> targetFieldPath) {

    public ParseRule {
      if (targetFieldPath != null) {
        if (targetFieldPath.isEmpty()) {
          throw new IllegalArgumentException("targetFieldPath must not be empty");
        }
        targetFieldPath = List.copyOf(targetFieldPath);
      }
    }

    public ParseRule(
        String targetField,
        boolean removeTargetField,
        ExtractionRule extractionRule,
        DispatchRule dispatchRule) {
      this(targetField, removeTargetField, extractionRule, dispatchRule, null);
    }

    public RecordFleakData parse(RecordFleakData input) {
      return parse(input, null, null);
    }

    public ParseResult parseDetailed(RecordFleakData input) {
      var events = new ArrayList<RuleEvent>();
      var output = parse(input, events, List.of());
      var root = events.getFirst();
      return new ParseResult(output, root.targetOutcome(), root.extractedKeys(), events);
    }

    private RecordFleakData parse(
        RecordFleakData input, List<RuleEvent> events, List<String> rulePath) {
      var parent = targetParent(input);
      var key = targetKey();
      var targetValue = parent == null ? null : parent.getPayload().get(key);
      // No trace objects, key sets or paths are allocated on the ordinary execution path.
      var kind = events == null ? null : inputKind(parent, key, targetValue);
      if (!(targetValue instanceof StringPrimitiveFleakData)
          || targetValue.getStringValue() == null
          || targetValue.getStringValue().isEmpty()) {
        if (events != null) {
          var outcome =
              switch (kind) {
                case MISSING -> TargetOutcome.SKIPPED_MISSING;
                case EMPTY_TEXT -> TargetOutcome.SKIPPED_EMPTY;
                default -> TargetOutcome.SKIPPED_NON_TEXT;
              };
          events.add(new RuleEvent(exactTargetPath(), kind, outcome, false, Set.of(), rulePath));
        }
        return input;
      }

      RecordFleakData mergedRecord;
      ParseRule nextRule;
      List<String> nextPath = null;
      try {
        var parsedData = extract(targetValue.getStringValue());
        var extractedKeys = events == null ? null : Set.copyOf(parsedData.getPayload().keySet());
        mergedRecord = merge(input, parsedData);
        if (events != null) {
          events.add(
              new RuleEvent(
                  exactTargetPath(),
                  kind,
                  extractedKeys.isEmpty()
                      ? TargetOutcome.NO_FIELDS_EXTRACTED
                      : TargetOutcome.EXTRACTED,
                  removeTargetField,
                  extractedKeys,
                  rulePath));
        }
        if (dispatchRule == null) return mergedRecord;
        var dispatchValue = mergedRecord.getPayload().get(dispatchRule.dispatchField);
        if (dispatchValue == null) return mergedRecord;
        var dispatchKey = dispatchValue.getStringValue();
        nextRule = dispatchRule.dispatchRulesMap.get(dispatchKey);
        boolean defaultRule = nextRule == null;
        if (defaultRule) nextRule = dispatchRule.defaultRule;
        if (nextRule == null) return mergedRecord;
        if (events != null) {
          nextPath = new ArrayList<>(rulePath);
          nextPath.add("dispatchConfig");
          if (defaultRule) nextPath.add("defaultConfig");
          else {
            nextPath.add("dispatchMap");
            nextPath.add(dispatchKey);
          }
        }
      } catch (RuntimeException failure) {
        if (events == null) throw failure;
        throw new DetailedParseException(failure, rulePath, exactTargetPath(), kind, events);
      }
      return nextRule.parse(mergedRecord, events, nextPath);
    }

    private RecordFleakData extract(String text) {
      try {
        return extractionRule.extract(text);
      } catch (Exception failure) {
        throw new RuntimeException("failed to parse input: " + failure.getMessage(), failure);
      }
    }

    private RecordFleakData targetParent(RecordFleakData input) {
      var parent = input;
      if (targetFieldPath != null) {
        for (int index = 0; index < targetFieldPath.size() - 1; index++) {
          var value = parent.getPayload().get(targetFieldPath.get(index));
          if (!(value instanceof RecordFleakData record)) return null;
          parent = record;
        }
      }
      return parent;
    }

    private String targetKey() {
      return targetFieldPath == null ? targetField : targetFieldPath.getLast();
    }

    private List<String> exactTargetPath() {
      return targetFieldPath == null ? List.of(targetField) : targetFieldPath;
    }

    private RecordFleakData merge(RecordFleakData input, RecordFleakData parsedData) {
      var merged = input.copy();
      if (removeTargetField) {
        var parent = merged;
        if (targetFieldPath != null) {
          // The successful read already proved these ancestors exist and are records.
          for (int index = 0; index < targetFieldPath.size() - 1; index++) {
            var key = targetFieldPath.get(index);
            var copy = ((RecordFleakData) parent.getPayload().get(key)).copy();
            parent.getPayload().put(key, copy);
            parent = copy;
          }
        }
        parent.getPayload().remove(targetKey());
      }
      merged.getPayload().putAll(parsedData.getPayload());
      return merged;
    }

    private static InputKind inputKind(RecordFleakData parent, String key, FleakData value) {
      if (parent == null || !parent.getPayload().containsKey(key)) return InputKind.MISSING;
      if (value == null) return InputKind.NULL;
      if (value instanceof StringPrimitiveFleakData text) {
        if (text.getStringValue() == null) return InputKind.NULL;
        return text.getStringValue().isEmpty() ? InputKind.EMPTY_TEXT : InputKind.TEXT;
      }
      return value instanceof RecordFleakData ? InputKind.OBJECT : InputKind.OTHER_NON_TEXT;
    }
  }

  record DispatchRule(
      @NonNull String dispatchField,
      @NonNull Map<String, ParseRule> dispatchRulesMap,
      ParseRule defaultRule) {}
}
