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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.databind.util.EnumResolver;
import com.google.common.primitives.Primitives;
import io.fleak.zephflow.api.ConfigurationPath;
import io.fleak.zephflow.api.ConfigurationValidationException;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Reads Jackson's structured failure metadata without changing the mapper's acceptance rules. */
public final class JsonConfigurationDiagnostics {
  private static final Map<Class<?>, String> SCALAR_TYPES =
      Map.of(
          String.class, "Expected a string",
          Character.class, "Expected a string",
          Boolean.class, "Expected a boolean (true or false)",
          Integer.class, "Expected an integer from -2147483648 to 2147483647",
          Long.class, "Expected an integer from -9223372036854775808 to 9223372036854775807",
          Short.class, "Expected an integer from -32768 to 32767",
          Byte.class, "Expected an integer from -128 to 127");

  private JsonConfigurationDiagnostics() {}

  /** Performs the existing conversion, adding diagnostics only after a recognized rejection. */
  public static <T> T convertValue(ObjectMapper mapper, Object value, Class<T> type) {
    try {
      return mapper.convertValue(value, type);
    } catch (IllegalArgumentException failure) {
      throw describe(mapper, value, failure);
    }
  }

  private static RuntimeException describe(
      ObjectMapper mapper, Object value, IllegalArgumentException failure) {
    if (failure instanceof ConfigurationValidationException) return failure;
    FailureContext context = failureContext(failure);
    ConfigurationPath path =
        context.mapping() == null ? ConfigurationPath.root() : path(context.mapping());
    if (context.configuration() != null) {
      return new ConfigurationValidationException(
          failure,
          context.configuration().issues().stream().map(issue -> issue.withPrefix(path)).toList());
    }
    return switch (context.mapping()) {
      case UnrecognizedPropertyException unknown ->
          ConfigurationValidationException.at(
              path,
              failure,
              "Unknown configuration field. Supported fields: "
                  + names(unknown.getKnownPropertyIds()));
      case InvalidTypeIdException invalidType -> describeTypeId(mapper, failure, path, invalidType);
      case ValueInstantiationException instantiation ->
          describeInstantiation(mapper, failure, path, instantiation);
      case MismatchedInputException mismatch -> describeMismatch(mapper, failure, path, mismatch);
      case null -> failure;
      default -> describeRejectedNull(mapper, value, failure, context.mapping(), path);
    };
  }

  private record FailureContext(
      JsonMappingException mapping, ConfigurationValidationException configuration) {}

  private static FailureContext failureContext(Throwable failure) {
    JsonMappingException mapping = null;
    ConfigurationValidationException configuration = null;
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable cause = failure; cause != null && visited.add(cause); cause = cause.getCause()) {
      if (mapping == null && cause instanceof JsonMappingException found) mapping = found;
      if (cause instanceof ConfigurationValidationException found) {
        configuration = found;
        break;
      }
    }
    return new FailureContext(mapping, configuration);
  }

  private static RuntimeException describeTypeId(
      ObjectMapper mapper,
      IllegalArgumentException failure,
      ConfigurationPath path,
      InvalidTypeIdException invalidType) {
    try {
      var deserializer =
          mapper.getDeserializationConfig().findTypeDeserializer(invalidType.getBaseType());
      if (deserializer != null) {
        String property = deserializer.getPropertyName();
        if (property != null && !property.isEmpty()) path = path.field(property);
        String accepted = deserializer.getTypeIdResolver().getDescForKnownTypeIds();
        return ConfigurationValidationException.at(
            path,
            failure,
            (invalidType.getTypeId() == null ? "Type is required." : "Unsupported type.")
                + (accepted == null ? "" : " Accepted values: " + accepted));
      }
    } catch (JsonMappingException unavailable) {
      // A mapper definition failure is not evidence of a particular invalid input field.
      return failure;
    }
    return failure;
  }

  private static RuntimeException describeInstantiation(
      ObjectMapper mapper,
      IllegalArgumentException failure,
      ConfigurationPath path,
      ValueInstantiationException instantiation) {
    if (instantiation.getType() != null && instantiation.getType().isEnumType()) {
      return ConfigurationValidationException.at(
          path,
          failure,
          "Unsupported value. Accepted values: "
              + enumNames(mapper, instantiation.getType().getRawClass()));
    }
    if (instantiation.getType() != null
        && instantiation.getType().getRawClass() == java.net.URI.class) {
      return ConfigurationValidationException.at(path, failure, "Expected a valid URI string.");
    }
    return failure;
  }

  private static RuntimeException describeMismatch(
      ObjectMapper mapper,
      IllegalArgumentException failure,
      ConfigurationPath path,
      MismatchedInputException mismatch) {
    Class<?> targetType = mismatch.getTargetType();
    if (targetType == null) return failure;
    if (targetType.isEnum()) {
      return ConfigurationValidationException.at(
          path, failure, "Unsupported value. Accepted values: " + enumNames(mapper, targetType));
    }
    String message =
        java.util.Date.class.isAssignableFrom(targetType)
            ? dateRequirement(mapper, mismatch)
            : expectedType(targetType);
    return ConfigurationValidationException.at(path, failure, message);
  }

  private static RuntimeException describeRejectedNull(
      ObjectMapper mapper,
      Object input,
      IllegalArgumentException failure,
      JsonMappingException mapping,
      ConfigurationPath path) {
    if (!(mapping.getCause() instanceof NullPointerException)
        || mapping.getPath().isEmpty()
        || !isExplicitNull(input, mapping.getPath())) return failure;
    String accepted =
        property(mapper, mapping)
            .map(BeanPropertyDefinition::getPrimaryType)
            .filter(type -> type.isEnumType())
            .map(type -> " Accepted values: " + enumNames(mapper, type.getRawClass()))
            .orElse("");
    return ConfigurationValidationException.at(
        path, failure, "Cannot read this field from null." + accepted);
  }

  private static boolean isExplicitNull(Object input, List<JsonMappingException.Reference> path) {
    Object value = input;
    for (var reference : path) {
      if (reference.getFieldName() != null
          && value instanceof Map<?, ?> map
          && map.containsKey(reference.getFieldName())) value = map.get(reference.getFieldName());
      else if (reference.getIndex() >= 0
          && value instanceof List<?> list
          && reference.getIndex() < list.size()) value = list.get(reference.getIndex());
      else return false;
    }
    return value == null;
  }

  private static Optional<BeanPropertyDefinition> property(
      ObjectMapper mapper, JsonMappingException mapping) {
    if (mapping.getPath().isEmpty()) return Optional.empty();
    var reference = mapping.getPath().getLast();
    Object owner = reference.getFrom();
    if (owner == null || reference.getFieldName() == null) return Optional.empty();
    Class<?> ownerType = owner instanceof Class<?> type ? type : owner.getClass();
    return mapper
        .getDeserializationConfig()
        .introspect(mapper.constructType(ownerType))
        .findProperties()
        .stream()
        .filter(candidate -> candidate.getName().equals(reference.getFieldName()))
        .findFirst();
  }

  private static String dateRequirement(ObjectMapper mapper, JsonMappingException mapping) {
    return property(mapper, mapping)
        .map(BeanPropertyDefinition::getPrimaryMember)
        .map(
            member ->
                mapper.getDeserializationConfig().getAnnotationIntrospector().findFormat(member))
        .filter(format -> format.hasPattern())
        .map(format -> "Expected a date string matching " + format.getPattern() + ".")
        .orElse("Expected a date in the configured date format.");
  }

  private static ConfigurationPath path(JsonMappingException failure) {
    ConfigurationPath path = ConfigurationPath.root();
    for (var reference : failure.getPath()) {
      if (reference.getFieldName() != null) path = path.field(reference.getFieldName());
      else if (reference.getIndex() >= 0) path = path.index(reference.getIndex());
    }
    return path;
  }

  private static String enumNames(ObjectMapper mapper, Class<?> type) {
    var config = mapper.getDeserializationConfig();
    var description = config.introspect(mapper.constructType(type));
    var accessor = description.findJsonValueAccessor();
    EnumResolver resolver;
    if (accessor != null) {
      if (config.canOverrideAccessModifiers()) accessor.fixAccess(true);
      resolver = EnumResolver.constructUsingMethod(config, description.getClassInfo(), accessor);
    } else if (config.isEnabled(DeserializationFeature.READ_ENUMS_USING_TO_STRING)) {
      resolver = EnumResolver.constructUsingToString(config, description.getClassInfo());
    } else {
      resolver = EnumResolver.constructFor(config, description.getClassInfo());
    }
    return names(resolver.getEnumIds());
  }

  private static String names(Collection<?> values) {
    return values == null
        ? "[]"
        : values.stream()
            .map(Object::toString)
            .sorted()
            .collect(Collectors.joining(", ", "[", "]"));
  }

  private static String expectedType(Class<?> type) {
    Class<?> boxedType = Primitives.wrap(type);
    String scalarType = SCALAR_TYPES.get(boxedType);
    if (scalarType != null) return scalarType;
    if (type == java.net.URI.class) return "Expected a valid URI string.";
    if (Number.class.isAssignableFrom(boxedType)) return "Expected a number";
    if (type.isArray() || Collection.class.isAssignableFrom(type)) return "Expected an array";
    if (Map.class.isAssignableFrom(type) || !type.isPrimitive())
      return "Expected an object matching the configuration structure";
    return "Value does not match the required configuration type";
  }
}
