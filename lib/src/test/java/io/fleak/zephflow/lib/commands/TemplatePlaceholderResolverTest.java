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
import io.fleak.zephflow.lib.commands.TemplatePlaceholderResolver.Escaping;
import io.fleak.zephflow.lib.commands.TemplatePlaceholderResolver.MissingValue;
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

  private static final FleakData URL_EVENT =
      FleakData.wrap(
          Map.of(
              "query", "a&b=c #d e/f*~",
              "url", "https://h.io/p?a=1",
              "host", "h.io:8080",
              "base", "https://h.io",
              "tenant", "acme",
              "evil", "evil.com#",
              "at", "@evil.com",
              "port", "8443"));

  private static String url(String template, FleakData event) {
    return TemplatePlaceholderResolver.create(template, Escaping.URL, MissingValue.RETURN_NULL)
        .resolveRequired(event);
  }

  @Test
  void urlEscapingPercentEncodesValuesAfterTheHost() {
    assertEquals(
        "https://x.io/s/a%26b%3Dc%20%23d%20e%2Ff%2A~?q=a%26b%3Dc%20%23d%20e%2Ff%2A~&k=1",
        url("https://x.io/s/{{$.query}}?q={{$.query}}&k=1", URL_EVENT));
  }

  @Test
  void urlEscapingKeepsWholeUrlBaseAndWholeHostPlaceholdersVerbatim() {
    assertEquals("https://h.io/p?a=1", url("{{$.url}}", URL_EVENT));
    assertEquals(
        "https://h.io/v1?x=https%3A%2F%2Fh.io", url("{{$.base}}/v1?x={{$.base}}", URL_EVENT));
    assertEquals(
        "https://h.io:8080/v1?x=h.io%3A8080", url("https://{{$.host}}/v1?x={{$.host}}", URL_EVENT));
  }

  @Test
  void urlEscapingAcceptsPlainValuesInsideTheHost() {
    assertEquals(
        "https://acme.api.example.com/v1",
        url("https://{{$.tenant}}.api.example.com/v1", URL_EVENT));
    assertEquals(
        "https://api.example.com:8443/x", url("https://api.example.com:{{$.port}}/x", URL_EVENT));
  }

  @Test
  void urlEscapingRejectsHostValuesThatWouldChangeTheTargetHost() {
    var shared =
        assertThrows(
            TemplateValueException.class,
            () -> url("https://{{$.evil}}.api.example.com/v1", URL_EVENT));
    assertTrue(shared.getMessage().contains("$.evil"));
    assertFalse(shared.getMessage().contains("evil.com#"));
    assertThrows(
        TemplateValueException.class, () -> url("https://api.example.com{{$.at}}", URL_EVENT));
  }

  @Test
  void strictResolutionNamesTheMissingPlaceholderWithoutEventValues() {
    var failure =
        assertThrows(
            TemplateValueException.class,
            () -> url("https://x.io/{{$.query}}/{{$.geo.country}}", URL_EVENT));
    assertTrue(failure.getMessage().contains("$.geo.country"));
    assertFalse(failure.getMessage().contains("a&b"));
  }

  @Test
  void renderEmptyTurnsMissingValuesIntoEmptyText() {
    assertEquals(
        "geo= end",
        TemplatePlaceholderResolver.create("geo={{$.geo}} end", Escaping.NONE, MissingValue.EMPTY)
            .resolvePlaceholders(URL_EVENT));
  }
}
