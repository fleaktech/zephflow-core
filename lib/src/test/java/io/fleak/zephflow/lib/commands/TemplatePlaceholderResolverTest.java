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

import io.fleak.zephflow.api.structure.FleakData;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Created by bolei on 8/29/24 */
class TemplatePlaceholderResolverTest {

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
    // Create a template with a placeholder that returns null
    String template = "Hello, {{ $.user.name }}!";

    // Spy on the StringTemplateResolver to inject mocked PathExpression
    TemplatePlaceholderResolver resolver = TemplatePlaceholderResolver.create(template);

    String result = resolver.resolvePlaceholders(FleakData.wrap(Map.of("k", 1)));
    assertEquals("Hello, null!", result);
  }
}
