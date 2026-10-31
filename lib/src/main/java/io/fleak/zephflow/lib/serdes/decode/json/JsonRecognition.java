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
package io.fleak.zephflow.lib.serdes.decode.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.InputStream;

public final class JsonRecognition {
  private JsonRecognition() {}

  public static JsonNode document(String text) throws IOException {
    try (JsonParser parser = JsonDecoding.FACTORY.createParser(text)) {
      JsonNode value = JsonDecoding.MAPPER.readTree(parser);
      if (parser.nextToken() != null)
        throw new IllegalArgumentException("Unexpected content after JSON document");
      return value;
    }
  }

  public static JsonParser parser(InputStream input) throws IOException {
    return JsonDecoding.FACTORY.createParser(input);
  }
}
