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
package io.fleak.zephflow.lib.commands;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonNode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.fleak.zephflow.api.ConfigurationPath;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.commands.TemplatePlaceholderResolver.Escaping;
import io.fleak.zephflow.lib.commands.TemplatePlaceholderResolver.MissingValue;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A JSON document with placeholders in its string values, rendered against an event into valid
 * JSON. The template is parsed once when created. A string that is exactly one placeholder takes
 * the value's own JSON type (null when the value is missing); a string mixing text and placeholders
 * stays a string, with missing values rendered as empty text. Object keys cannot hold placeholders.
 */
public final class JsonTemplate {
  private static final String INVALID_JSON =
      "Template must be valid JSON, with each placeholder inside quotes, for example"
          + " \"{{$.field}}\".";
  private static final String PLACEHOLDER_IN_KEY =
      "Template placeholders are not supported in JSON object keys.";

  private final Part root;

  private JsonTemplate(Part root) {
    this.root = root;
  }

  public static JsonTemplate create(String template, ConfigurationPath path) {
    JsonNode tree;
    try {
      tree = OBJECT_MAPPER.readTree(template);
    } catch (JsonProcessingException failure) {
      throw ConfigurationValidationException.at(
          path, new IllegalArgumentException(failure.getOriginalMessage(), failure), INVALID_JSON);
    }
    if (tree == null || tree.isMissingNode()) {
      throw ConfigurationValidationException.at(
          path, new IllegalArgumentException("empty JSON template"), INVALID_JSON);
    }
    return new JsonTemplate(compile(tree, path));
  }

  public String render(FleakData event) {
    try {
      return OBJECT_MAPPER.writeValueAsString(root.render(event));
    } catch (JsonProcessingException failure) {
      throw new UncheckedIOException(failure);
    }
  }

  private static Part compile(JsonNode node, ConfigurationPath path) {
    if (node.isObject()) {
      Map<String, Part> fields = new LinkedHashMap<>();
      for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
        Map.Entry<String, JsonNode> field = it.next();
        if (!TemplatePlaceholderResolver.create(field.getKey(), path).placeholders().isEmpty()) {
          throw ConfigurationValidationException.at(
              path, new IllegalArgumentException(PLACEHOLDER_IN_KEY), PLACEHOLDER_IN_KEY);
        }
        fields.put(field.getKey(), compile(field.getValue(), path));
      }
      return allLiteral(fields.values()) ? new Literal(node) : new ObjectPart(fields);
    }
    if (node.isArray()) {
      List<Part> elements = new ArrayList<>();
      node.forEach(element -> elements.add(compile(element, path)));
      return allLiteral(elements) ? new Literal(node) : new ArrayPart(elements);
    }
    if (node.isTextual()) {
      String text = node.textValue();
      TemplatePlaceholderResolver resolver =
          TemplatePlaceholderResolver.create(text, path, Escaping.NONE, MissingValue.EMPTY);
      List<TemplatePlaceholderResolver.Placeholder> placeholders = resolver.placeholders();
      if (placeholders.isEmpty()) {
        return new Literal(node);
      }
      if (placeholders.size() == 1
          && placeholders.getFirst().start() == 0
          && placeholders.getFirst().end() == text.length()) {
        return new Slot(placeholders.getFirst().pathExpression());
      }
      return new MixedString(resolver);
    }
    return new Literal(node);
  }

  private static boolean allLiteral(Collection<Part> parts) {
    return parts.stream().allMatch(part -> part instanceof Literal);
  }

  private sealed interface Part permits Literal, Slot, MixedString, ObjectPart, ArrayPart {
    JsonNode render(FleakData event);
  }

  private record Literal(JsonNode value) implements Part {
    @Override
    public JsonNode render(FleakData event) {
      return value;
    }
  }

  private record Slot(PathExpression pathExpression) implements Part {
    @Override
    public JsonNode render(FleakData event) {
      FleakData value = pathExpression.calculateValue(event);
      return value == null ? NullNode.getInstance() : toJsonNode(value);
    }
  }

  private record MixedString(TemplatePlaceholderResolver resolver) implements Part {
    @Override
    public JsonNode render(FleakData event) {
      return TextNode.valueOf(resolver.resolvePlaceholders(event));
    }
  }

  private record ObjectPart(Map<String, Part> fields) implements Part {
    @Override
    public JsonNode render(FleakData event) {
      ObjectNode object = OBJECT_MAPPER.createObjectNode();
      fields.forEach((name, part) -> object.set(name, part.render(event)));
      return object;
    }
  }

  private record ArrayPart(List<Part> elements) implements Part {
    @Override
    public JsonNode render(FleakData event) {
      ArrayNode array = OBJECT_MAPPER.createArrayNode();
      elements.forEach(part -> array.add(part.render(event)));
      return array;
    }
  }
}
