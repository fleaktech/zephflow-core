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
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Isolated runner regressions: no application context, database or external services. */
public final class QueueWorkerFixtures {
  private static final java.io.PrintStream savedOut = System.out;
  private static final java.io.PrintStream savedErr = System.err;

  public static class LoggingFirst {
    @Test
    void logs() {
      savedOut.println("first class standard output");
      savedErr.println("first class standard error");
    }
  }

  public static class LoggingSecond {
    @Test
    void logs() {
      savedOut.println("second class standard output");
      savedErr.println("second class standard error");
    }
  }

  public static class FailingSetup {
    @BeforeAll
    static void prepare() {
      throw new IllegalStateException("fixture setup failed");
    }

    @Test
    void firstMethod() {}

    @Test
    void secondMethod() {}
  }

  public static class FailingCleanup {
    @Test
    void completesBeforeCleanup() {}

    @AfterAll
    static void cleanup() {
      throw new IllegalStateException("fixture cleanup failed");
    }
  }

  public static class Passing {
    @Test
    void succeeds() {
      System.out.println("fixture standard output <text>");
      System.err.println("fixture standard error <text>");
    }
  }

  public static class AbortedSetup {
    @BeforeAll
    static void prepare() {
      org.junit.jupiter.api.Assumptions.assumeTrue(false, "fixture unavailable credentials");
    }

    @Test
    void firstMethod() {
      fail("must be skipped");
    }

    @Nested
    class NestedTests {
      @Test
      void nestedMethod() {
        fail("must be skipped");
      }
    }
  }

  public static class Mixed {
    @Test
    void ordinary() {}

    @Test
    @Tag("golden")
    void goldenMethod() {}

    @Nested
    class NestedTests {
      @Test
      void nested() {}

      @Test
      @Tag("golden")
      void nestedGolden() {}
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void parameterized(int value) {
      assertTrue(value > 0);
    }

    @TestFactory
    Stream<DynamicTest> generated() {
      return Stream.of("first", "second").map(name -> DynamicTest.dynamicTest(name, () -> {}));
    }

    @RepeatedTest(2)
    void repeated() {}

    @Test
    @Disabled("fixture skip")
    void disabled() {}
  }

  @Tag("golden")
  public static class Golden {
    @Test
    void fixture() {}
  }

  @Execution(ExecutionMode.CONCURRENT)
  public static class Sequential {
    private static final AtomicInteger active = new AtomicInteger();
    private static final AtomicInteger maximum = new AtomicInteger();

    @RepeatedTest(8)
    void observesOneMethodAtATime() throws Exception {
      maximum.accumulateAndGet(active.incrementAndGet(), Math::max);
      try {
        Thread.sleep(30);
      } finally {
        active.decrementAndGet();
      }
    }

    @AfterAll
    static void checkSequentialExecution() {
      assertEquals(1, maximum.get());
    }
  }

  public static class Slow {
    @Test
    void waitsForCoordinatorInterruption() throws Exception {
      Files.writeString(Path.of("slow-started"), Long.toString(ProcessHandle.current().pid()));
      Thread.sleep(60_000);
    }
  }
}
