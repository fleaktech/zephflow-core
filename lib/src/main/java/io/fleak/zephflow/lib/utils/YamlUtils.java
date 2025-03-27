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
package io.fleak.zephflow.lib.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/** Created by bolei on 2/28/25 */
public interface YamlUtils {
  ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  static <T> T fromYamlResource(String resourcePath, TypeReference<T> typeReference)
      throws IOException {
    try (InputStream in = YamlUtils.class.getResourceAsStream(resourcePath)) {
      return fromYamlInputStream(in, typeReference);
    }
  }

  static <T> T fromYamlInputStream(InputStream in, TypeReference<T> typeReference) {
    try {
      return OBJECT_MAPPER.readValue(in, typeReference);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static <T> T fromYamlString(String str, TypeReference<T> typeReference) {
    try {
      return OBJECT_MAPPER.readValue(str, typeReference);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static String toYamlString(Object object) {
    if (Objects.isNull(object)) {
      return null;
    }
    try {
      return OBJECT_MAPPER.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
