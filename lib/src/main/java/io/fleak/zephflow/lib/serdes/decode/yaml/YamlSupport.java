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

import java.time.LocalDate;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.ConstructorException;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;

/**
 * SnakeYAML used directly, because Jackson's YAML reader turns aliases into the anchor's name and
 * cannot tell a timestamp from a quoted string. Only the safe constructor is used, duplicate keys
 * are errors, and the size and alias limits are lifted so that the decoder's budget and {@link
 * YamlRecords} decide.
 */
final class YamlSupport {

  private static final Pattern DATE = Pattern.compile("^([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})$");

  private YamlSupport() {}

  /** A new instance per cursor: SnakeYAML objects are not thread-safe. */
  static Yaml yaml() {
    LoaderOptions options = new LoaderOptions();
    options.setAllowDuplicateKeys(false);
    options.setCodePointLimit(Integer.MAX_VALUE);
    // SnakeYAML counts aliases over the whole stream, not per document; YamlRecords caps expansion.
    options.setMaxAliasesForCollections(Integer.MAX_VALUE);
    options.setNestingDepthLimit(1000);
    return new Yaml(new DateAwareConstructor(options));
  }

  /**
   * A date without time becomes a {@link LocalDate}, so it is not mistaken for a timestamp. Any
   * failure while constructing a fully composed document is reported as a {@link
   * ConstructorException}, so a multi-document stream can go on with the next document.
   */
  private static final class DateAwareConstructor extends SafeConstructor {

    DateAwareConstructor(LoaderOptions options) {
      super(options);
      ConstructYamlTimestamp timestamp = new ConstructYamlTimestamp();
      yamlConstructors.put(
          Tag.TIMESTAMP,
          new AbstractConstruct() {
            @Override
            public Object construct(Node node) {
              Matcher date = DATE.matcher(((ScalarNode) node).getValue());
              if (date.matches()) {
                return LocalDate.of(
                    Integer.parseInt(date.group(1)),
                    Integer.parseInt(date.group(2)),
                    Integer.parseInt(date.group(3)));
              }
              return timestamp.construct(node);
            }
          });
    }

    @Override
    public Object getData() {
      if (!composer.checkNode()) {
        throw new NoSuchElementException("No document is available.");
      }
      Node node = composer.getNode();
      try {
        return constructDocument(node);
      } catch (ConstructorException e) {
        throw e;
      } catch (RuntimeException e) {
        throw new DocumentConstructionException(node, e);
      }
    }
  }

  private static final class DocumentConstructionException extends ConstructorException {

    DocumentConstructionException(Node node, RuntimeException cause) {
      super(
          null,
          null,
          cause.getClass().getSimpleName() + ": " + cause.getMessage(),
          node.getStartMark(),
          cause);
    }
  }
}
