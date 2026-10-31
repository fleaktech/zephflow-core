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
package io.fleak.zephflow.lib.serdes.decode.yaml;

import java.io.BufferedReader;
import java.io.FilterReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.events.DocumentStartEvent;
import org.yaml.snakeyaml.events.MappingStartEvent;
import org.yaml.snakeyaml.events.SequenceStartEvent;

public final class YamlRecognition {
  private YamlRecognition() {}

  public static boolean candidate(InputStream input) throws IOException {
    int value;
    while ((value = input.read()) >= 0) {
      if (value == '#') {
        while ((value = input.read()) >= 0 && value != '\n') {}
        continue;
      }
      if (value == ' ' || value == '\n' || value == '\r' || value == '\t') continue;
      if (value == 0xEF) {
        if (input.read() == 0xBB && input.read() == 0xBF) continue;
        return false;
      }
      if ("[{-?%!&\"'".indexOf(value) >= 0) return true;
      while (value >= 0 && value != '\n' && value != '\r') {
        if (value == ':') {
          value = input.read();
          if (value < 0 || value == ' ' || value == '\t' || value == '\n' || value == '\r')
            return true;
        } else value = input.read();
      }
      return false;
    }
    return false;
  }

  public static int documentCount(InputStream input, long byteLimit) throws IOException {
    try {
      var reader =
          new BufferedReader(
              new InputStreamReader(
                  input,
                  StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)));
      int documents = 0;
      boolean recognized = false;
      for (var event :
          YamlSupport.yaml(YamlRecords.maxValues(byteLimit)).parse(completeReads(reader))) {
        if (event instanceof DocumentStartEvent document) {
          documents++;
          recognized |= document.getExplicit();
          if (documents > 1) return documents;
        } else if (event instanceof MappingStartEvent || event instanceof SequenceStartEvent) {
          recognized = true;
        }
      }
      return recognized ? documents : 0;
    } catch (YAMLException exception) {
      Throwable cause = exception;
      while (cause.getCause() != null) cause = cause.getCause();
      if (cause instanceof IOException io
          && !(io instanceof java.nio.charset.CharacterCodingException)) throw io;
      return 0;
    }
  }

  private static Reader completeReads(Reader reader) {
    return new FilterReader(reader) {
      @Override
      public int read(char[] characters, int offset, int length) throws IOException {
        int total = 0;
        while (total < length) {
          int count = super.read(characters, offset + total, length - total);
          if (count < 0) return total == 0 ? -1 : total;
          total += count;
        }
        return total;
      }
    };
  }

  public static boolean matches(String text, long byteLimit) {
    try {
      Object document = YamlSupport.yaml(YamlRecords.maxValues(byteLimit)).load(text);
      if (document instanceof Map<?, ?> || document instanceof Collection<?>) return true;
      for (var event :
          YamlSupport.yaml(YamlRecords.maxValues(byteLimit)).parse(new StringReader(text))) {
        if (event instanceof DocumentStartEvent start) return start.getExplicit();
      }
      return false;
    } catch (RuntimeException exception) {
      return false;
    }
  }
}
