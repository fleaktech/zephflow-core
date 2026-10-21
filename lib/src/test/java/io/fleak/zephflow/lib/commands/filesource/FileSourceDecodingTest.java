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
package io.fleak.zephflow.lib.commands.filesource;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.commands.kafkasource.KafkaSourceCommandTest;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

class FileSourceDecodingTest {

  @TempDir Path tempDir;

  @Test
  void decodingAloneDecodesTheFile() throws IOException {
    Path file = write("{\"a\":1}\n{\"a\":2}\n".getBytes(StandardCharsets.UTF_8));

    List<RecordFleakData> records =
        run(
            TestUtils.JOB_CONTEXT,
            FileSourceDto.Config.builder()
                .filePath(file.toString())
                .decoding(
                    new DecodingConfig(
                        List.of(), new JsonFormatConfig(JsonFormatConfig.Mode.LINES)))
                .build());

    assertEquals(List.of(record(1), record(2)), records);
  }

  @Test
  void encodingTypeDecodesAGzipFileAsItIs() throws IOException {
    byte[] gzipped = gzip("{\"a\":1}");

    DeadLettered result = runWithDlq(gzipped);

    assertEquals(
        new DeadLettered(List.of(), new SerializedEvent(null, gzipped, null).toString()),
        new DeadLettered(result.records(), result.value()));
    assertEquals("MALFORMED json file: JsonParseException", result.lines().getFirst());
  }

  @Test
  void aBadFileIsDeadLetteredWithTheFileTexts() throws IOException {
    byte[] bad = "not json".getBytes(StandardCharsets.UTF_8);

    DeadLettered result = runWithDlq(bad);

    assertEquals(new SerializedEvent(null, bad, null).toString(), result.value());
    List<String> lines = result.lines();
    assertEquals(
        List.of(
            "MALFORMED json file: JsonParseException",
            "Action: check that the file is json as the source's encodingType or decoding says,"
                + " and that its compression is set when the file is compressed; fix the file and"
                + " run the job again",
            "com.fasterxml.jackson.core.JsonParseException",
            "bytes 0-8"),
        List.of(lines.get(0), lines.get(1), lines.get(2).split(":")[0], lines.getLast()));
  }

  /** What a JSON_OBJECT file gave: its records, and its one DLQ entry's value and text lines. */
  private record DeadLettered(List<RecordFleakData> records, String value, List<String> lines) {
    DeadLettered(List<RecordFleakData> records, String value) {
      this(records, value, List.of());
    }
  }

  private DeadLettered runWithDlq(byte[] content) throws IOException {
    Path file = write(content);
    JobContext jobContext = TestUtils.buildJobContext(new HashMap<>());
    jobContext.setDlqConfig(JobContext.S3DlqConfig.builder().build());
    DlqWriter dlqWriter = mock(DlqWriter.class);
    List<RecordFleakData> records;
    try (var dlqFactory = mockStatic(DlqWriterFactory.class)) {
      dlqFactory
          .when(() -> DlqWriterFactory.createDlqWriter(jobContext.getDlqConfig(), null))
          .thenReturn(dlqWriter);
      dlqFactory
          .when(() -> DlqWriterFactory.createSampleWriter(jobContext.getDlqConfig(), null))
          .thenReturn(mock(DlqWriter.class));
      records =
          run(
              jobContext,
              FileSourceDto.Config.builder()
                  .filePath(file.toString())
                  .encodingType(EncodingType.JSON_OBJECT)
                  .build());
    }
    ArgumentCaptor<SerializedEvent> value = ArgumentCaptor.forClass(SerializedEvent.class);
    ArgumentCaptor<String> text = ArgumentCaptor.forClass(String.class);
    verify(dlqWriter).writeToDlq(anyLong(), value.capture(), text.capture(), eq("node"));
    return new DeadLettered(records, value.getValue().toString(), text.getValue().lines().toList());
  }

  private List<RecordFleakData> run(JobContext jobContext, FileSourceDto.Config config) {
    FileSourceCommand command =
        new FileSourceCommand(
            "node",
            jobContext,
            new JsonConfigParser<>(FileSourceDto.Config.class),
            new FileSourceConfigValidator());
    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    KafkaSourceCommandTest.TestSourceEventAcceptor acceptor =
        new KafkaSourceCommandTest.TestSourceEventAcceptor();
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    command.execute("user", acceptor);
    return acceptor.getReceivedEvents();
  }

  private Path write(byte[] bytes) throws IOException {
    Path file = tempDir.resolve("input");
    Files.write(file, bytes);
    return file;
  }

  private static RecordFleakData record(int a) {
    return (RecordFleakData) FleakData.wrap(Map.of("a", a));
  }

  private static byte[] gzip(String text) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
      gzip.write(text.getBytes(StandardCharsets.UTF_8));
    }
    return out.toByteArray();
  }
}
