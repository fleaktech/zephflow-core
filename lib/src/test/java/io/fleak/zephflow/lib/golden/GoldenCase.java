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
package io.fleak.zephflow.lib.golden;

import java.nio.file.Path;

/**
 * One fixture directory:
 *
 * <pre>
 * golden/&lt;command&gt;/&lt;case-name&gt;/
 *   README.md        why this case exists, who reviewed expected.jsonl and against what
 *   config.json      the command's config, exactly as it would appear in a DAG
 *   input.jsonl      one JSON record per line, in processing order
 *   expected.jsonl   one JSON record per line, the human-reviewed output
 *   expected_errors.jsonl   (optional) records the command is expected to reject
 * </pre>
 *
 * Control fields in input.jsonl (stripped before the record reaches the command):
 *
 * <ul>
 *   <li>{@code "_t": 60000} — set the injected clock to this many ms after epoch 0 before
 *       processing this record
 *   <li>{@code {"_advance": 30000}} — a record with only this field advances the clock and sends
 *       nothing
 * </ul>
 */
public record GoldenCase(String command, String name, Path dir) {
  public Path config() {
    return dir.resolve("config.json");
  }

  public Path input() {
    return dir.resolve("input.jsonl");
  }

  public Path expected() {
    return dir.resolve("expected.jsonl");
  }

  public Path expectedErrors() {
    return dir.resolve("expected_errors.jsonl");
  }

  public String displayName() {
    return command + "/" + name;
  }
}
