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
import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import javax.xml.stream.*;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.*;
import org.junit.platform.launcher.core.*;

/** Persistent JUnit worker. Control traffic is separate from test stdout and stderr. */
public final class QueueWorker {
  private record CaseResult(
      String id,
      String name,
      String owner,
      String className,
      double seconds,
      String status,
      Throwable failure) {}

  private static final Map<String, CaseResult> results = new LinkedHashMap<>();
  private static final Map<String, Long> started = new HashMap<>();
  private static final List<Throwable> sessionFailures = new ArrayList<>();
  private static int opened;
  private static int closed;
  private static int duplicates;
  private static long discoveryNanos;
  private static long executionNanos;
  private static int requestCount;
  private static TestPlan currentPlan;
  private static String currentClass = "unknown";

  private static String owner(TestIdentifier identifier) {
    return identifier.getUniqueIdObject().getSegments().stream()
        .filter(segment -> segment.getType().equals("class"))
        .map(segment -> segment.getValue())
        .findFirst()
        .orElse(currentClass);
  }

  private static String className(TestIdentifier identifier) {
    var source = identifier.getSource().orElse(null);
    if (source instanceof org.junit.platform.engine.support.descriptor.MethodSource method) {
      return method.getClassName();
    }
    if (source instanceof org.junit.platform.engine.support.descriptor.ClassSource type) {
      return type.getClassName();
    }
    return currentPlan.getParent(identifier).map(QueueWorker::className).orElse(owner(identifier));
  }

  private static final TestExecutionListener listener =
      new TestExecutionListener() {
        @Override
        public void executionStarted(TestIdentifier identifier) {
          started.put(identifier.getUniqueId(), System.nanoTime());
        }

        @Override
        public void executionSkipped(TestIdentifier identifier, String reason) {
          if (identifier.isTest()) record(identifier, "SKIPPED", null, 0);
          else {
            var descendants =
                currentPlan.getDescendants(identifier).stream()
                    .filter(TestIdentifier::isTest)
                    .toList();
            for (var child : descendants) record(child, "SKIPPED", null, 0);
            if (descendants.isEmpty()
                && identifier
                    .getSource()
                    .filter(
                        org.junit.platform.engine.support.descriptor.MethodSource.class::isInstance)
                    .isPresent()) {
              record(identifier, "SKIPPED", null, 0);
            }
          }
        }

        @Override
        public void executionFinished(TestIdentifier identifier, TestExecutionResult result) {
          if (identifier.isTest()) {
            long start = started.getOrDefault(identifier.getUniqueId(), System.nanoTime());
            record(
                identifier,
                result.getStatus().name(),
                result.getThrowable().orElse(null),
                (System.nanoTime() - start) / 1e9);
          } else if (result.getStatus() != TestExecutionResult.Status.SUCCESSFUL) {
            if (result.getStatus() == TestExecutionResult.Status.FAILED) {
              record(identifier, "ERROR", result.getThrowable().orElseThrow(), 0);
            }
            // Failed or aborted @BeforeAll prevents JUnit from executing the methods below it.
            for (var child : currentPlan.getDescendants(identifier)) {
              if (child.isTest() && !results.containsKey(child.getUniqueId())) {
                record(child, "SKIPPED", null, 0);
              }
            }
          }
          started.remove(identifier.getUniqueId());
        }

        private void record(
            TestIdentifier identifier, String status, Throwable failure, double seconds) {
          String id = identifier.getUniqueId();
          if (results.put(
                  id,
                  new CaseResult(
                      id,
                      identifier.getDisplayName(),
                      owner(identifier),
                      className(identifier),
                      seconds,
                      status,
                      failure))
              != null) duplicates++;
        }
      };

  private static LauncherDiscoveryRequestBuilder request() {
    var builder =
        LauncherDiscoveryRequestBuilder.request()
            .configurationParameter("junit.jupiter.execution.parallel.enabled", "false");
    String included = System.getProperty("test.queue.includeTags", "");
    String excluded = System.getProperty("test.queue.excludeTags", "");
    if (!included.isEmpty()) builder.filters(TagFilter.includeTags(included.split("\n")));
    if (!excluded.isEmpty()) builder.filters(TagFilter.excludeTags(excluded.split("\n")));
    return builder;
  }

  private static void execute(Launcher launcher, String name) {
    currentClass = name;
    long start = System.nanoTime();
    var request = request().selectors(DiscoverySelectors.selectClass(name)).build();
    var plan = launcher.discover(request);
    currentPlan = plan;
    discoveryNanos += System.nanoTime() - start;
    start = System.nanoTime();
    launcher.execute(plan, listener);
    executionNanos += System.nanoTime() - start;
  }

  private static void writeReport(Path directory, String owner) throws Exception {
    Files.createDirectories(directory);
    List<CaseResult> cases =
        results.values().stream().filter(result -> result.owner().equals(owner)).toList();
    if (cases.isEmpty()) return;
    Path report = directory.resolve("TEST-" + owner + ".xml");
    Path temporary = directory.resolve("TEST-" + owner + ".xml.tmp");
    try (var output = Files.newOutputStream(temporary)) {
      var xml = XMLOutputFactory.newFactory().createXMLStreamWriter(output, "UTF-8");
      xml.writeStartDocument("UTF-8", "1.0");
      xml.writeStartElement("testsuite");
      xml.writeAttribute("name", owner);
      xml.writeAttribute("tests", Integer.toString(cases.size()));
      xml.writeAttribute(
          "failures",
          Long.toString(cases.stream().filter(r -> r.status().equals("FAILED")).count()));
      xml.writeAttribute(
          "errors", Long.toString(cases.stream().filter(r -> r.status().equals("ERROR")).count()));
      xml.writeAttribute(
          "skipped",
          Long.toString(
              cases.stream()
                  .filter(r -> r.status().equals("SKIPPED") || r.status().equals("ABORTED"))
                  .count()));
      for (CaseResult result : cases) {
        xml.writeStartElement("testcase");
        xml.writeAttribute("classname", result.className());
        xml.writeAttribute("name", result.name());
        xml.writeAttribute("unique-id", result.id());
        xml.writeAttribute("time", Double.toString(result.seconds()));
        if (result.status().equals("FAILED") || result.status().equals("ERROR")) {
          xml.writeStartElement(result.status().equals("ERROR") ? "error" : "failure");
          xml.writeAttribute("message", String.valueOf(result.failure()));
          var stack = new StringWriter();
          result.failure().printStackTrace(new PrintWriter(stack));
          xml.writeCharacters(stack.toString());
          xml.writeEndElement();
        } else if (result.status().equals("SKIPPED") || result.status().equals("ABORTED")) {
          xml.writeEmptyElement("skipped");
        }
        xml.writeEndElement();
      }
      for (String stream : List.of("out", "err")) {
        xml.writeStartElement("system-" + stream);
        try (var reader = Files.newBufferedReader(directory.resolve(owner + ".std" + stream))) {
          char[] buffer = new char[8192];
          for (int count; (count = reader.read(buffer)) != -1; ) {
            xml.writeCharacters(buffer, 0, count);
          }
        }
        xml.writeEndElement();
      }
      xml.writeEndElement();
      xml.writeEndDocument();
      xml.close();
    }
    Files.move(
        temporary, report, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
  }

  // Console loggers may retain System.out. Keep one stream for the entire JVM session.
  private static final class CapturedOutput extends OutputStream {
    private final PrintStream original;
    private OutputStream file;

    CapturedOutput(PrintStream original) {
      this.original = original;
    }

    synchronized void start(Path destination) throws IOException {
      file = Files.newOutputStream(destination);
    }

    @Override
    public synchronized void write(int value) throws IOException {
      original.write(value);
      if (file != null) file.write(value);
    }

    @Override
    public synchronized void write(byte[] bytes, int offset, int length) throws IOException {
      original.write(bytes, offset, length);
      if (file != null) file.write(bytes, offset, length);
    }

    @Override
    public synchronized void flush() throws IOException {
      original.flush();
      if (file != null) file.flush();
    }

    @Override
    public synchronized void close() throws IOException {
      if (file != null) {
        file.close();
        file = null;
      }
    }
  }

  public static void main(String[] arguments) throws Exception {
    if (arguments[0].equals("discover")) {
      var names = Files.readAllLines(Path.of(arguments[1]));
      try (var session = LauncherFactory.openSession()) {
        var request =
            request()
                .selectors(names.stream().map(DiscoverySelectors::selectClass).toList())
                .build();
        var plan = session.getLauncher().discover(request);
        var classes = new TreeSet<String>();
        var tests = new TreeSet<String>();
        for (var root : plan.getRoots()) {
          for (var identifier : plan.getDescendants(root)) {
            String owner = owner(identifier);
            if (!owner.equals("unknown")) classes.add(owner);
            if (identifier.isTest()) tests.add(identifier.getUniqueId());
          }
        }
        Files.write(Path.of(arguments[2]), classes);
        Files.write(Path.of(arguments[2] + ".static-ids"), tests);
        System.out.println("DISCOVERED classes=" + classes.size() + " staticTests=" + tests.size());
      }
      return;
    }
    if (!arguments[0].equals("run")) throw new IllegalArgumentException("Expected discover or run");
    Path directory = Path.of(arguments[1]);
    int exitCode = 0;
    Files.createDirectories(directory);
    var out = new CapturedOutput(System.out);
    var err = new CapturedOutput(System.err);
    System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
    System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
    // Local test-runner IPC on loopback; the coordinator checks the per-run token below.
    // nosemgrep: java.lang.security.audit.crypto.unencrypted-socket.unencrypted-socket
    try (var socket = new Socket("127.0.0.1", Integer.parseInt(arguments[2]));
        var input =
            new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        var control = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8)) {
      socket.setTcpNoDelay(true);
      var sessions =
          new LauncherSessionListener() {
            @Override
            public void launcherSessionOpened(LauncherSession session) {
              opened++;
            }

            @Override
            public void launcherSessionClosed(LauncherSession session) {
              closed++;
            }
          };
      var configuration = LauncherConfig.builder().addLauncherSessionListeners(sessions).build();
      try (var session = LauncherFactory.openSession(configuration)) {
        control.println("READY\t" + arguments[3] + "\t" + arguments[4]);
        boolean stopped = false;
        for (String command; (command = input.readLine()) != null; ) {
          if (command.equals("STOP")) {
            stopped = true;
            break;
          }
          String[] parts = command.split("\t", 2);
          String name = parts[1];
          if (name.contains("/") || name.contains("\\") || name.contains("..")) {
            throw new IllegalArgumentException("Invalid test class name");
          }
          long started = System.nanoTime();
          try {
            out.start(directory.resolve(name + ".stdout"));
            err.start(directory.resolve(name + ".stderr"));
            execute(session.getLauncher(), name);
          } finally {
            out.close();
            err.close();
            // Write each class once; keep its results if a later class crashes this JVM.
            writeReport(directory, name);
          }
          requestCount++;
          control.println(
              "DONE\t" + parts[0] + "\t" + (System.nanoTime() - started) + "\t" + results.size());
        }
        if (!stopped) throw new EOFException("Coordinator disconnected before STOP");
      } catch (Throwable failure) {
        failure.printStackTrace();
        sessionFailures.add(failure);
      }
      try (var errorLog =
          new PrintWriter(Files.newBufferedWriter(directory.resolve("session-errors.log")))) {
        for (var failure : sessionFailures) failure.printStackTrace(errorLog);
      }
      long failed =
          results.values().stream()
              .filter(result -> result.status().equals("FAILED") || result.status().equals("ERROR"))
              .count();
      if (failed > 0 || !sessionFailures.isEmpty() || duplicates > 0 || opened != 1 || closed != 1)
        exitCode = 1;
      String summary =
          String.join(
              "\t",
              "SUMMARY",
              Integer.toString(results.size()),
              Long.toString(failed),
              Integer.toString(sessionFailures.size()),
              Integer.toString(duplicates),
              Integer.toString(opened),
              Integer.toString(closed),
              Long.toString(discoveryNanos),
              Long.toString(executionNanos),
              Integer.toString(requestCount),
              Integer.toString(exitCode),
              Long.toString(
                  results.values().stream()
                      .filter(
                          result ->
                              result.status().equals("SKIPPED")
                                  || result.status().equals("ABORTED"))
                      .count()));
      Files.writeString(directory.resolve("summary.tsv"), summary + "\n");
      control.println(summary);
    }
    try (var threads =
        new PrintWriter(Files.newBufferedWriter(directory.resolve("remaining-threads.log")))) {
      Thread.getAllStackTraces()
          .forEach(
              (thread, stack) -> {
                if (thread != Thread.currentThread() && thread.isAlive() && !thread.isDaemon()) {
                  threads.println(thread.getName() + " " + thread.getState());
                  for (var frame : stack) threads.println("  at " + frame);
                }
              });
    }
    System.exit(exitCode);
  }
}
