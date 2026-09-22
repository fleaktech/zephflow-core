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

import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestFactory;

/**
 * Discovers every fixture under src/test/resources/golden/&lt;command&gt;/&lt;case&gt;/ and runs
 * it.
 *
 * <pre>
 *   ./gradlew :lib:goldenTest                                   # all fixtures
 *   ./gradlew :lib:goldenTest -Dgolden.filter=parser/            # one command
 *   ./gradlew :lib:goldenTest -Dgolden.filter=cisco-asa          # substring match
 *   ./gradlew :lib:goldenTest -Dgolden.filter=cisco-asa -Dgolden.update=true
 *       # rewrites expected.jsonl, then FAILS on purpose: go read `git diff` and review it
 * </pre>
 */
@Tag("golden")
class GoldenTest {

  // Source tree, not build/resources: update mode must write where git can see it.
  private static final Path ROOT =
      Path.of(System.getProperty("golden.dir", "src/test/resources/golden"));

  @TestFactory
  Stream<DynamicTest> fixtures() throws IOException {
    String filter = System.getProperty("golden.filter", "");
    List<GoldenCase> cases = discover();
    if (cases.isEmpty()) fail("no golden fixtures found under " + ROOT.toAbsolutePath());
    return cases.stream()
        .filter(c -> c.displayName().contains(filter))
        .map(c -> DynamicTest.dynamicTest(c.displayName(), () -> runOne(c)));
  }

  private static List<GoldenCase> discover() throws IOException {
    List<GoldenCase> out = new ArrayList<>();
    if (!Files.isDirectory(ROOT)) return out;
    try (Stream<Path> commands = Files.list(ROOT)) {
      for (Path cmdDir : commands.filter(Files::isDirectory).sorted().toList()) {
        try (Stream<Path> caseDirs = Files.list(cmdDir)) {
          for (Path caseDir : caseDirs.filter(Files::isDirectory).sorted().toList()) {
            if (Files.exists(caseDir.resolve("input.jsonl"))) {
              out.add(
                  new GoldenCase(
                      cmdDir.getFileName().toString(), caseDir.getFileName().toString(), caseDir));
            }
          }
        }
      }
    }
    return out;
  }

  private static void runOne(GoldenCase c) throws IOException {
    GoldenRunner.Result actual = GoldenRunner.run(c);

    if (GoldenSupport.UPDATE) {
      GoldenSupport.writeJsonl(c.expected(), actual.output());
      if (!actual.errors().isEmpty()) GoldenSupport.writeJsonl(c.expectedErrors(), actual.errors());
      else Files.deleteIfExists(c.expectedErrors());
      fail(
          "golden.update: rewrote "
              + c.expected()
              + " — review `git diff` and commit it together with the code change");
    }

    if (!Files.exists(c.expected())) {
      fail(
          c.displayName()
              + ": expected.jsonl missing. Generate with -Dgolden.update=true, then review it.");
    }

    List<String> diffs = new ArrayList<>();
    List<JsonNode> actualOut = new ArrayList<>();
    for (var n : actual.output())
      actualOut.add(GoldenSupport.readJsonl0(GoldenSupport.stableLine(n)));
    diffs.addAll(GoldenSupport.diff("output", GoldenSupport.readJsonl(c.expected()), actualOut));

    List<JsonNode> actualErr = new ArrayList<>();
    for (var n : actual.errors())
      actualErr.add(GoldenSupport.readJsonl0(GoldenSupport.stableLine(n)));
    diffs.addAll(
        GoldenSupport.diff("errors", GoldenSupport.readJsonl(c.expectedErrors()), actualErr));

    if (!diffs.isEmpty()) {
      fail(
          c.displayName()
              + " differs from expected ("
              + c.dir()
              + "):\n  "
              + String.join("\n  ", diffs)
              + "\nIf the change is intended: rerun with -Dgolden.update=true and review the diff.");
    }
  }
}
