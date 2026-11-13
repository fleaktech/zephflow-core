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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.ConfigurationPath;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.api.structure.FleakData;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Created by bolei on 8/29/24 */
class TemplatePlaceholderResolverTest {

  @Test
  void locatesAnInvalidPlaceholderWithoutChangingTheLegacyFailure() {
    String template = "do-not-copy-this {{ $.message extra }}";
    var path = ConfigurationPath.of("provider", "headerTemplates", "x.test");
    var legacy =
        assertThrows(RuntimeException.class, () -> TemplatePlaceholderResolver.create(template));
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () -> TemplatePlaceholderResolver.create(template, path));
    assertEquals(path, failure.issues().getFirst().path());
    assertTrue(
        failure.issues().getFirst().message().contains("placeholder starting at character 17"));
    assertTrue(failure.issues().getFirst().message().contains("expected end of expression"));
    assertFalse(failure.issues().getFirst().message().contains("do-not-copy-this"));
    assertEquals(legacy.getClass(), failure.originalFailure().getClass());
    assertEquals(legacy.getMessage(), failure.originalFailure().getMessage());
    assertEquals(legacy.getCause().getClass(), failure.originalFailure().getCause().getClass());
    assertEquals(
        "hello event",
        TemplatePlaceholderResolver.create("hello {{ $.name }}", path)
            .resolvePlaceholders(FleakData.wrap(Map.of("name", "event"))));
  }

  @Test
  void locatesANullTemplateAndRetainsTheNullPointerFailure() {
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () ->
                TemplatePlaceholderResolver.create(
                    null, ConfigurationPath.of("headerTemplates", "x.test")));
    assertEquals(NullPointerException.class, failure.originalFailure().getClass());
    assertEquals(
        ConfigurationPath.of("headerTemplates", "x.test"), failure.issues().getFirst().path());
  }

  @Test
  public void testResolveTemplate_withPlaceholders() {
    // Create a template with placeholders
    String template = "Hello, {{ $.name }}! Your balance is {{ $.balance }}.";

    // Spy on the StringTemplateResolver to inject mocked PathExpressions
    TemplatePlaceholderResolver resolver = TemplatePlaceholderResolver.create(template);

    // Resolve the template
    String result =
        resolver.resolvePlaceholders(
            FleakData.wrap(Map.of("name", "John Doe", "balance", "$1000")));

    // Verify the result
    assertEquals("Hello, John Doe! Your balance is $1000.", result);
  }

  @Test
  public void testResolveTemplate_withoutPlaceholders() {
    // Create a template without placeholders
    String template = "Hello, World!";

    // Instantiate the resolver
    TemplatePlaceholderResolver resolver = TemplatePlaceholderResolver.create(template);

    // Resolve the template
    String result = resolver.resolvePlaceholders(FleakData.wrap(Map.of("k", 1)));

    // Verify the result (should be the same as the template)
    assertEquals(template, result);
  }

  @Test
  public void testResolveTemplate_withEmptyTemplate() {
    // Create an empty template
    String template = "";

    // Instantiate the resolver
    TemplatePlaceholderResolver resolver = TemplatePlaceholderResolver.create(template);

    // Resolve the template
    String result = resolver.resolvePlaceholders(FleakData.wrap(Map.of("k", 1)));

    // Verify the result (should be an empty string)
    assertEquals("", result);
  }

  @Test
  public void testResolveTemplate_withUnresolvedPlaceholder() {
    String template = "Hello, {{ $.user.name }}!";
    TemplatePlaceholderResolver resolver = TemplatePlaceholderResolver.create(template);

    String result = resolver.resolvePlaceholders(FleakData.wrap(Map.of("k", 1)));
    assertNull(result);
  }

  @Test
  public void testResolveTemplate_oneOfMultiplePlaceholdersUnresolved() {
    String template = "{{$.a}}-{{$.b}}";
    TemplatePlaceholderResolver resolver = TemplatePlaceholderResolver.create(template);

    String result = resolver.resolvePlaceholders(FleakData.wrap(Map.of("a", "x")));
    assertNull(result);
  }
}
