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
package io.fleak.zephflow.lib.commands.databrickssink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.databricks.sdk.service.sql.StatementState;
import io.delta.kernel.types.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.databrickssink.DatabricksSqlExecutor.CopyIntoStats;
import io.fleak.zephflow.lib.commands.databrickssink.DatabricksSqlExecutor.StatementExecutionException;
import io.fleak.zephflow.lib.commands.deltalakesink.InvalidRecordException;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class BatchDatabricksRecoveryTest {
  @TempDir Path temporaryDirectory;

  private static final StructType SCHEMA =
      new StructType()
          .add("id", IntegerType.INTEGER, false)
          .add("payload", new StructType().add("count", LongType.LONG, true), true);

  @Test
  void isolatesLazyParquetFailureAndDeliversTheNextBatch() throws Exception {
    try (Fixture fixture = new Fixture(3, true)) {
      var result = fixture.flush(List.of(record(1, 10), record(2, "not-a-number"), record(3, 30)));
      assertEquals(2, result.successCount());
      assertEquals(List.of(1, 3), fixture.committed);
      assertEquals(1, result.errorOutputList().size());
      assertEquals(
          FleakData.wrap(record(2, "not-a-number")),
          result.errorOutputList().getFirst().inputEvent());
      assertTrue(result.flushedDataSize() > 0);
      assertTrue(fixture.attemptDirectories.stream().noneMatch(Files::exists));

      var next = fixture.flush(List.of(record(4, 40), record(5, 50), record(6, 60)));
      assertEquals(3, next.successCount());
      assertEquals(List.of(1, 3, 4, 5, 6), fixture.committed);
      assertTrue(next.errorOutputList().isEmpty());
    }
  }

  @Test
  void isolatesInvalidMapValueAndDeliversPopulatedAndEmptyMaps() throws Exception {
    StructType schema =
        new StructType()
            .add("id", IntegerType.INTEGER, false)
            .add("attributes", new MapType(StringType.STRING, StringType.STRING, false), true);
    try (Fixture fixture = new Fixture(3, true, schema)) {
      Map<String, String> invalidMap = new HashMap<>();
      invalidMap.put("region", null);
      var before = Map.<String, Object>of("id", 1, "attributes", Map.of("region", "eu"));
      var invalid = Map.<String, Object>of("id", 2, "attributes", invalidMap);
      var after = Map.<String, Object>of("id", 3, "attributes", Map.of());
      Map<Integer, Map<String, String>> deliveredMaps = new HashMap<>();
      doAnswer(
              invocation -> {
                File file = invocation.getArgument(0);
                fixture.recordUpload(file, invocation.getArgument(1));
                try (ParquetReader<Group> reader =
                    ParquetReader.builder(
                            new GroupReadSupport(), new org.apache.hadoop.fs.Path(file.toURI()))
                        .build()) {
                  Group row;
                  while ((row = reader.read()) != null) {
                    Group map = row.getGroup("attributes", 0);
                    Map<String, String> values = new HashMap<>();
                    for (int entry = 0; entry < map.getFieldRepetitionCount("key_value"); entry++) {
                      Group pair = map.getGroup("key_value", entry);
                      values.put(pair.getString("key", 0), pair.getString("value", 0));
                    }
                    assertNull(deliveredMaps.put(row.getInteger("id", 0), values));
                  }
                }
                return null;
              })
          .when(fixture.uploader)
          .uploadFile(any(), anyString());
      var result = fixture.flush(List.of(before, invalid, after));
      assertEquals(2, result.successCount());
      assertEquals(Map.of(1, Map.of("region", "eu"), 3, Map.of()), deliveredMaps);
      assertEquals(List.of(1, 3), fixture.committed);
      assertEquals(1, result.errorOutputList().size());
      assertEquals(FleakData.wrap(invalid), result.errorOutputList().getFirst().inputEvent());
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"structMissing", "structNull", "arrayNull", "mapStructMissing", "mapArrayNull"})
  void isolatesRequiredNestedNullWithoutCorruptingNeighbors(String variant) throws Exception {
    boolean map = variant.startsWith("map");
    boolean array = variant.toLowerCase(Locale.ROOT).contains("array");
    DataType nested =
        array
            ? new ArrayType(LongType.LONG, false)
            : new StructType().add("count", LongType.LONG, false);
    DataType valueType = map ? new MapType(StringType.STRING, nested, false) : nested;
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER, false).add("value", valueType, false);
    Object beforeValue = array ? List.of(1L) : Map.of("count", 1L);
    Object afterValue = array ? List.of(3L) : Map.of("count", 3L);
    Object invalidValue =
        array
            ? Arrays.asList(2L, null)
            : variant.equals("structNull") ? Collections.singletonMap("count", null) : Map.of();
    if (map) {
      beforeValue = Map.of("key", beforeValue);
      afterValue = Map.of("key", afterValue);
      invalidValue = Map.of("key", invalidValue);
    }
    var before = Map.<String, Object>of("id", 1, "value", beforeValue);
    var invalid = Map.<String, Object>of("id", 2, "value", invalidValue);
    var after = Map.<String, Object>of("id", 3, "value", afterValue);
    Map<Integer, Long> deliveredValues = new HashMap<>();
    try (Fixture fixture = new Fixture(3, true, schema)) {
      doAnswer(
              invocation -> {
                File file = invocation.getArgument(0);
                fixture.recordUpload(file, invocation.getArgument(1));
                try (ParquetReader<Group> reader =
                    ParquetReader.builder(
                            new GroupReadSupport(), new org.apache.hadoop.fs.Path(file.toURI()))
                        .build()) {
                  Group row;
                  while ((row = reader.read()) != null) {
                    Group value = row.getGroup("value", 0);
                    if (map) value = value.getGroup("key_value", 0).getGroup("value", 0);
                    long count =
                        array
                            ? value.getGroup("list", 0).getLong("element", 0)
                            : value.getLong("count", 0);
                    assertNull(deliveredValues.put(row.getInteger("id", 0), count));
                  }
                }
                return null;
              })
          .when(fixture.uploader)
          .uploadFile(any(), anyString());
      var result = fixture.flush(List.of(before, invalid, after));
      assertEquals(2, result.successCount());
      assertEquals(List.of(1, 3), fixture.committed);
      assertEquals(Map.of(1, 1L, 3, 3L), deliveredValues);
      assertEquals(1, result.errorOutputList().size());
      assertEquals(FleakData.wrap(invalid), result.errorOutputList().getFirst().inputEvent());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {9, 18})
  void isolatesDecimalEncodingScaleOverflow(int precision) throws Exception {
    StructType schema =
        new StructType()
            .add("id", IntegerType.INTEGER, false)
            .add("value", new DecimalType(precision, 2), false);
    var invalid = Map.<String, Object>of("id", 2, "value", "1e2147483647");
    Map<Integer, Long> deliveredValues = new HashMap<>();
    try (Fixture fixture = new Fixture(3, true, schema)) {
      doAnswer(
              invocation -> {
                File file = invocation.getArgument(0);
                fixture.recordUpload(file, invocation.getArgument(1));
                try (ParquetReader<Group> reader =
                    ParquetReader.builder(
                            new GroupReadSupport(), new org.apache.hadoop.fs.Path(file.toURI()))
                        .build()) {
                  Group row;
                  while ((row = reader.read()) != null) {
                    long unscaled =
                        precision <= 9 ? row.getInteger("value", 0) : row.getLong("value", 0);
                    assertNull(deliveredValues.put(row.getInteger("id", 0), unscaled));
                  }
                }
                return null;
              })
          .when(fixture.uploader)
          .uploadFile(any(), anyString());
      var result =
          fixture.flush(
              List.of(Map.of("id", 1, "value", "1.00"), invalid, Map.of("id", 3, "value", "3.00")));
      assertEquals(2, result.successCount());
      assertEquals(List.of(1, 3), fixture.committed);
      assertEquals(Map.of(1, 100L, 3, 300L), deliveredValues);
      assertEquals(1, result.errorOutputList().size());
      assertEquals(FleakData.wrap(invalid), result.errorOutputList().getFirst().inputEvent());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void isolatesWrongNestedStructShapeWithoutCorruptingNeighbors(boolean array) throws Exception {
    StructType inner = new StructType().add("count", LongType.LONG, false);
    DataType nested = array ? new ArrayType(inner, false) : inner;
    StructType schema =
        new StructType()
            .add("id", IntegerType.INTEGER, false)
            .add("value", new StructType().add("inner", nested, false), false);
    var before =
        Map.<String, Object>of(
            "id",
            1,
            "value",
            Map.of("inner", array ? List.of(Map.of("count", 11L)) : Map.of("count", 11L)));
    var invalid =
        Map.<String, Object>of("id", 2, "value", Map.of("inner", array ? List.of("bad") : "bad"));
    var after =
        Map.<String, Object>of(
            "id",
            3,
            "value",
            Map.of("inner", array ? List.of(Map.of("count", 33L)) : Map.of("count", 33L)));
    Map<Integer, Long> deliveredValues = new HashMap<>();
    try (Fixture fixture = new Fixture(3, true, schema)) {
      doAnswer(
              invocation -> {
                File file = invocation.getArgument(0);
                fixture.recordUpload(file, invocation.getArgument(1));
                try (ParquetReader<Group> reader =
                    ParquetReader.builder(
                            new GroupReadSupport(), new org.apache.hadoop.fs.Path(file.toURI()))
                        .build()) {
                  Group row;
                  while ((row = reader.read()) != null) {
                    Group value = row.getGroup("value", 0).getGroup("inner", 0);
                    if (array) value = value.getGroup("list", 0).getGroup("element", 0);
                    assertNull(
                        deliveredValues.put(row.getInteger("id", 0), value.getLong("count", 0)));
                  }
                }
                return null;
              })
          .when(fixture.uploader)
          .uploadFile(any(), anyString());
      var result = fixture.flush(List.of(before, invalid, after));
      assertEquals(2, result.successCount());
      assertEquals(List.of(1, 3), fixture.committed);
      assertEquals(Map.of(1, 11L, 3, 33L), deliveredValues);
      assertEquals(1, result.errorOutputList().size());
      assertEquals(FleakData.wrap(invalid), result.errorOutputList().getFirst().inputEvent());
    }
  }

  @Test
  void preservesEqualRecordsAsSeparateOccurrences() throws Exception {
    try (Fixture fixture = new Fixture(3, true)) {
      Map<String, Object> good = record(1, 10);
      var result = fixture.flush(List.of(good, record(2, "bad"), good));
      assertEquals(2, result.successCount());
      assertEquals(List.of(1, 1), fixture.committed);
      assertEquals(1, result.errorOutputList().size());
    }
  }

  @Test
  void rejectsAllBadRecordsWithoutUploading() throws Exception {
    try (Fixture fixture = new Fixture(3, true)) {
      var input = List.of(record(1, "bad"), record(2, "bad"), record(3, "bad"));
      var result = fixture.flush(input);
      assertEquals(0, result.successCount());
      assertEquals(
          input.stream().map(FleakData::wrap).toList(),
          result.errorOutputList().stream().map(error -> error.inputEvent()).toList());
      verifyNoInteractions(fixture.uploader, fixture.sql);
      verify(fixture.writer, atMost(5)).writeParquetFiles(anyList(), any());
    }
  }

  @Test
  void isolatesFailureAfterTheFirstConversionChunkAndRemovesPartialFiles() throws Exception {
    try (Fixture fixture = new Fixture(1002, true)) {
      List<Map<String, Object>> input = new ArrayList<>();
      for (int id = 0; id < 1002; id++) {
        input.add(record(id, id == 1000 ? "bad" : id));
      }
      List<Path> partialDirectories = new ArrayList<>();
      doAnswer(
              invocation -> {
                Path directory = invocation.getArgument(1);
                fixture.attemptDirectories.add(directory);
                try {
                  return invocation.callRealMethod();
                } catch (Exception failure) {
                  try (var paths = Files.list(directory)) {
                    if (paths.findAny().isPresent()) partialDirectories.add(directory);
                  }
                  throw failure;
                }
              })
          .when(fixture.writer)
          .writeParquetFiles(anyList(), any());
      var result = fixture.flush(input);
      assertEquals(1001, result.successCount());
      assertEquals(1001, fixture.committed.size());
      assertFalse(fixture.committed.contains(1000));
      assertEquals(1001, new HashSet<>(fixture.committed).size());
      assertEquals(1, result.errorOutputList().size());
      assertFalse(partialDirectories.isEmpty(), "The regression must leave a real partial file");
      assertTrue(partialDirectories.stream().noneMatch(Files::exists));
      assertEquals(1, fixture.copies.size());
    }
  }

  @Test
  void neverUploadsCompletedOrPartialFilesFromAFailedMultiFileAttempt() throws Exception {
    try (Fixture fixture = new Fixture(3, true)) {
      DatabricksParquetWriter actualWriter = new DatabricksParquetWriter(SCHEMA);
      List<Path> failedDirectories = new ArrayList<>();
      doAnswer(
              invocation -> {
                List<Map<String, Object>> records = invocation.getArgument(0);
                Path directory = invocation.getArgument(1);
                fixture.attemptDirectories.add(directory);
                List<File> files = new ArrayList<>();
                try {
                  for (Map<String, Object> record : records) {
                    files.addAll(actualWriter.writeParquetFiles(List.of(record), directory));
                  }
                  return files;
                } catch (Exception failure) {
                  if (!files.isEmpty()) failedDirectories.add(directory);
                  throw failure;
                }
              })
          .when(fixture.writer)
          .writeParquetFiles(anyList(), any());
      var result = fixture.flush(List.of(record(1, 1), record(2, "bad"), record(3, 3)));
      assertEquals(List.of(1, 3), fixture.committed);
      assertEquals(2, result.successCount());
      assertEquals(1, result.errorOutputList().size());
      assertFalse(failedDirectories.isEmpty());
      assertTrue(failedDirectories.stream().noneMatch(Files::exists));
      verify(fixture.uploader, times(2)).uploadFile(any(), anyString());
      assertEquals(List.of(List.of(1, 3)), fixture.copies);
    }
  }

  @Test
  void doesNotBisectIoOrUnclassifiedFailures() throws Exception {
    InvalidRecordException dataWithIo = new InvalidRecordException("data");
    dataWithIo.addSuppressed(new IOException("cleanup failed"));
    List<Exception> failures =
        List.of(
            new IOException("disk full"),
            new UncheckedIOException(new IOException("disk full")),
            new UnsupportedOperationException("getter implementation"),
            new IllegalArgumentException("unclassified"),
            dataWithIo);
    for (Exception failure : failures) {
      try (Fixture fixture = new Fixture(3, true)) {
        doAnswer(
                invocation -> {
                  Path directory = invocation.getArgument(1);
                  Files.writeString(directory.resolve("partial.parquet"), "partial");
                  fixture.attemptDirectories.add(directory);
                  throw failure;
                })
            .when(fixture.writer)
            .writeParquetFiles(anyList(), any());
        var result = fixture.flush(List.of(record(1, 1), record(2, 2), record(3, 3)));
        assertEquals(3, result.errorOutputList().size());
        assertEquals(0, result.successCount());
        verify(fixture.writer).writeParquetFiles(anyList(), any());
        verifyNoInteractions(fixture.uploader, fixture.sql);
        assertTrue(fixture.attemptDirectories.stream().noneMatch(Files::exists));
      }
    }
  }

  @Test
  void preservesEarlierLocalErrorWhenALaterAttemptHasAnIoFailure() throws Exception {
    try (Fixture fixture = new Fixture(3, true)) {
      doAnswer(
              invocation -> {
                List<Map<String, Object>> values = invocation.getArgument(0);
                if (values.size() == 2) throw new IOException("disk full");
                return invocation.callRealMethod();
              })
          .when(fixture.writer)
          .writeParquetFiles(anyList(), any());
      var result = fixture.flush(List.of(record(1, "bad"), record(2, 2), record(3, 3)));
      assertEquals(3, result.errorOutputList().size());
      assertTrue(
          result.errorOutputList().get(0).errorMessage().contains("Parquet conversion failed"));
      assertTrue(result.errorOutputList().get(1).errorMessage().contains("disk full"));
      verifyNoInteractions(fixture.uploader, fixture.sql);
    }
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"23001", "23502", "22012", "22023", "42501", "opaque"})
  void isolatesEveryTerminalValidationFailure(String sqlState) throws Exception {
    try (Fixture fixture = new Fixture(3, true)) {
      fixture.validationBehavior =
          ids -> {
            if (ids.contains(2)) throw rejection(sqlState);
          };
      var result = fixture.flush(List.of(record(1, 1), record(2, 2), record(3, 3)));
      assertEquals(List.of(1, 3), fixture.committed);
      assertEquals(2, result.successCount());
      assertEquals(1, result.errorOutputList().size());
      assertEquals(FleakData.wrap(record(2, 2)), result.errorOutputList().getFirst().inputEvent());
      assertEquals(
          List.of(List.of(1, 2, 3), List.of(1), List.of(2, 3), List.of(2), List.of(3)),
          fixture.validations);
      assertEquals(List.of(List.of(1), List.of(3)), fixture.copies);
      assertEquals(fixture.validations.size(), fixture.validationDirectories.size());
      assertEquals(fixture.validationDirectories, new HashSet<>(fixture.deletedDirectories));
      assertEquals(fixture.committedBytes, result.flushedDataSize());
    }
  }

  @Test
  void mergesLocalAndRemoteErrorsWithoutLosingCommittedNeighbors() throws Exception {
    try (Fixture fixture = new Fixture(4, true)) {
      fixture.validationBehavior =
          ids -> {
            if (ids.contains(3)) throw rejection("23001");
          };
      var input = List.of(record(1, 1), record(2, "bad"), record(3, 3), record(4, 4));
      var result = fixture.flush(input);
      assertEquals(List.of(1, 4), fixture.committed);
      assertEquals(2, result.successCount());
      assertEquals(
          List.of(FleakData.wrap(input.get(1)), FleakData.wrap(input.get(2))),
          result.errorOutputList().stream().map(error -> error.inputEvent()).toList());
    }
  }

  @Test
  void preservesCommittedSiblingAfterUnknownOutcomeAndKeepsItsRemoteSource() throws Exception {
    Fixture fixture = new Fixture(3, true);
    fixture.validationBehavior =
        ids -> {
          if (ids.size() == 3) throw rejection("23001");
        };
    fixture.copyBehavior =
        ids -> {
          if (ids.contains(2)) throw unknown();
          return stats(ids.size());
        };
    var result = fixture.flush(List.of(record(1, 1), record(2, 2), record(3, 3)));
    assertEquals(List.of(1), fixture.committed);
    assertEquals(1, result.successCount());
    assertEquals(2, result.errorOutputList().size());
    assertTrue(
        result.errorOutputList().stream()
            .allMatch(error -> error.errorMessage().contains("outcome unknown")));
    assertEquals(2, fixture.copies.size());
    assertEquals(fixture.committedBytes, result.flushedDataSize());
    Set<String> retained = new HashSet<>(fixture.copyDirectories);
    retained.removeAll(fixture.deletedDirectories);
    assertEquals(1, retained.size());
    fixture.close();
    assertTrue(Collections.disjoint(retained, fixture.deletedDirectories));
  }

  @Test
  void stopsBeforeUnattemptedSiblingsAfterAnUnknownSingleton() throws Exception {
    try (Fixture fixture = new Fixture(4, true)) {
      fixture.validationBehavior =
          ids -> {
            if (ids.contains(2) && ids.size() > 1) throw rejection("23001");
          };
      fixture.copyBehavior =
          ids -> {
            if (ids.contains(2)) throw unknown();
            return stats(ids.size());
          };
      var result = fixture.flush(List.of(record(1, 1), record(2, 2), record(3, 3), record(4, 4)));
      assertEquals(List.of(1), fixture.committed);
      assertEquals(1, result.successCount());
      assertEquals(3, result.errorOutputList().size());
      assertTrue(result.errorOutputList().get(0).errorMessage().contains("outcome unknown"));
      assertTrue(result.errorOutputList().get(1).errorMessage().contains("not attempted"));
      assertTrue(result.errorOutputList().get(2).errorMessage().contains("not attempted"));
      assertEquals(
          List.of(List.of(1, 2, 3, 4), List.of(1, 2), List.of(1), List.of(2)), fixture.validations);
    }
  }

  @Test
  void reportsEveryRemoteRejectedOccurrenceExactlyOnce() throws Exception {
    try (Fixture fixture = new Fixture(3, true)) {
      fixture.validationBehavior =
          ids -> {
            throw rejection("23001");
          };
      var input = List.of(record(1, 1), record(2, 2), record(1, 1));
      var result = fixture.flush(input);
      assertEquals(0, result.successCount());
      assertEquals(0, result.flushedDataSize());
      assertEquals(
          input.stream().map(FleakData::wrap).toList(),
          result.errorOutputList().stream().map(error -> error.inputEvent()).toList());
      assertEquals(5, fixture.validations.size());
      assertTrue(fixture.copies.isEmpty());
      assertTrue(fixture.committed.isEmpty());
    }
  }

  @Test
  void preservesCommittedSiblingAfterUploadFailureAndDoesNotAttemptOtherGroups() throws Exception {
    try (Fixture fixture = new Fixture(3, true)) {
      fixture.validationBehavior =
          ids -> {
            if (ids.size() == 3) throw rejection("23001");
          };
      doAnswer(
              invocation -> {
                File file = invocation.getArgument(0);
                List<Integer> ids = readIds(file);
                if (ids.equals(List.of(2, 3))) throw new IOException("upload denied");
                fixture.recordUpload(file, invocation.getArgument(1));
                return null;
              })
          .when(fixture.uploader)
          .uploadFile(any(), anyString());
      var result = fixture.flush(List.of(record(1, 1), record(2, 2), record(3, 3)));
      assertEquals(List.of(1), fixture.committed);
      assertEquals(1, result.successCount());
      assertEquals(2, result.errorOutputList().size());
      assertTrue(result.errorOutputList().getFirst().errorMessage().contains("upload denied"));
      assertEquals(1, fixture.copies.size());
      assertEquals(fixture.committedBytes, result.flushedDataSize());
    }
  }

  @Test
  void doesNotReplayOperationalOrUnclassifiedSqlErrors() throws Exception {
    List<RuntimeException> failures =
        List.of(
            new StatementExecutionException(
                "statement",
                StatementState.FAILED,
                "42501",
                "PERMISSION_DENIED",
                "permission denied",
                false,
                null),
            new StatementExecutionException(
                "statement", StatementState.CANCELED, null, null, "canceled", false, null),
            unknown(),
            new RuntimeException("lost response"));
    for (RuntimeException failure : failures) {
      try (Fixture fixture = new Fixture(3, true)) {
        fixture.copyBehavior =
            ids -> {
              throw failure;
            };
        var result = fixture.flush(List.of(record(1, 1), record(2, 2), record(3, 3)));
        assertEquals(3, result.errorOutputList().size());
        assertEquals(1, fixture.copies.size());
        verify(fixture.writer).writeParquetFiles(anyList(), any());
        assertEquals(0, fixture.deletedDirectories.size());
        assertTrue(
            result.errorOutputList().stream()
                .allMatch(error -> error.errorMessage().contains("outcome unknown")));
      }
    }
  }

  @Test
  void validatesEveryRecordBeyondThePreviewWindow() throws Exception {
    List<Map<String, Object>> input = new ArrayList<>();
    List<Integer> expected = new ArrayList<>();
    for (int id = 1; id <= 61; id++) {
      input.add(record(id, id));
      if (id != 55) expected.add(id);
    }
    try (Fixture fixture = new Fixture(61, true)) {
      fixture.validationBehavior =
          ids -> {
            if (ids.contains(55)) throw rejection("22023");
          };
      var result = fixture.flush(input);
      assertEquals(expected, fixture.committed);
      assertEquals(60, result.successCount());
      assertEquals(61, fixture.validations.getFirst().size());
      assertEquals(1, result.errorOutputList().size());
      assertEquals(FleakData.wrap(input.get(54)), result.errorOutputList().getFirst().inputEvent());
    }
  }

  @Test
  void stopsAfterUnconfirmedOrCanceledValidationWithoutAnyMutation() throws Exception {
    List<RuntimeException> failures =
        List.of(
            unknown(),
            new StatementExecutionException(
                "validation",
                StatementState.CANCELED,
                null,
                null,
                "validation canceled",
                false,
                null),
            new RuntimeException("validation response lost"));
    for (RuntimeException failure : failures) {
      try (Fixture fixture = new Fixture(3, true)) {
        fixture.validationBehavior =
            ids -> {
              throw failure;
            };
        var result = fixture.flush(List.of(record(1, 1), record(2, 2), record(3, 3)));
        assertEquals(3, result.errorOutputList().size());
        assertTrue(
            result.errorOutputList().stream()
                .allMatch(error -> error.errorMessage().contains("validation failed")));
        assertEquals(1, fixture.validations.size());
        assertTrue(fixture.copies.isEmpty());
        assertTrue(fixture.committed.isEmpty());
        assertEquals(fixture.validationDirectories, new HashSet<>(fixture.deletedDirectories));
        verify(fixture.writer).writeParquetFiles(anyList(), any());
      }
    }
  }

  @Test
  void postCommitHookFailureNeverReplaysPersistedRowsAndRetainsSource() throws Exception {
    Fixture fixture = new Fixture(3, true);
    List<Integer> persisted = new ArrayList<>();
    fixture.copyBehavior =
        ids -> {
          persisted.addAll(ids);
          throw new StatementExecutionException(
              "copy",
              StatementState.FAILED,
              "2DKD0",
              "INTERNAL_ERROR",
              "[DELTA_POST_COMMIT_HOOK_FAILED] Commit succeeded before hook failed",
              false,
              null);
        };
    var result = fixture.flush(List.of(record(1, 1), record(2, 2), record(3, 3)));
    assertEquals(List.of(1, 2, 3), persisted);
    assertEquals(1, fixture.copies.size());
    assertEquals(1, fixture.validations.size());
    assertEquals(0, result.successCount());
    assertEquals(3, result.errorOutputList().size());
    assertTrue(
        result.errorOutputList().stream()
            .allMatch(error -> error.errorMessage().contains("outcome unknown")));
    assertTrue(fixture.deletedDirectories.isEmpty());
    fixture.close();
    assertTrue(fixture.deletedDirectories.isEmpty());
  }

  @Test
  void treatsCommittedWithoutRowCountAsCommittedAndNeverReplays() throws Exception {
    try (Fixture fixture = new Fixture(3, true)) {
      fixture.copyBehavior = ids -> new CopyIntoStats(0, 0, 0, List.of(), false);
      var result = fixture.flush(List.of(record(1, 1), record(2, 2), record(3, 3)));
      assertEquals(List.of(1, 2, 3), fixture.committed);
      assertTrue(result.errorOutputList().isEmpty());
      assertEquals(0, result.successCount());
      assertTrue(result.flushedDataSize() > 0);
      assertEquals(1, fixture.copies.size());
    }
  }

  @Test
  void cleanupFailureAfterCommitDoesNotReplayOrRejectRecords() throws Exception {
    try (Fixture fixture = new Fixture(3, true)) {
      doThrow(new IllegalStateException("cleanup denied"))
          .when(fixture.uploader)
          .deleteDirectory(anyString());
      var result = fixture.flush(List.of(record(1, 1), record(2, 2), record(3, 3)));
      assertEquals(3, result.successCount());
      assertTrue(result.errorOutputList().isEmpty());
      assertEquals(1, fixture.copies.size());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void cleanupFlagKeepsOnlyFinalLocalFiles(boolean cleanup) throws Exception {
    try (Fixture fixture = new Fixture(3, cleanup)) {
      var result = fixture.flush(List.of(record(1, 1), record(2, "bad"), record(3, 3)));
      assertEquals(2, result.successCount());
      long retained = fixture.attemptDirectories.stream().filter(Files::exists).count();
      assertEquals(cleanup ? 0 : 2, retained);
      assertEquals(cleanup ? 1 : 0, fixture.deletedDirectories.size());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"scheduled", "close"})
  void outOfBandFlushPreservesOriginsAndCountsSuccessExactlyOnce(String lifecycle)
      throws Exception {
    Fixture fixture = new Fixture(100, true);
    fixture.validationBehavior =
        ids -> {
          if (ids.contains(3)) throw rejection("23001");
        };
    var input = List.of(record(1, 1), record(2, "bad"), record(3, 3), record(4, 4));
    assertEquals(0, fixture.flush(input).successCount());
    if (lifecycle.equals("scheduled")) fixture.flusher.executeScheduledFlush();
    else fixture.close();
    assertEquals(List.of(1, 4), fixture.committed);
    verify(fixture.outputCounter).increase(2, Map.of());
    verify(fixture.errorCounter).increase(2, Map.of());
    verify(fixture.dlq)
        .writeToDlq(
            anyLong(),
            argThat(event -> matchesRecord(event, input.get(1))),
            contains("Parquet conversion failed"),
            eq("sink"));
    verify(fixture.dlq)
        .writeToDlq(
            anyLong(),
            argThat(event -> matchesRecord(event, input.get(2))),
            contains("COPY INTO validation rejected record"),
            eq("sink"));
    fixture.close();
    verify(fixture.outputCounter).increase(2, Map.of());
  }

  private static boolean matchesRecord(
      io.fleak.zephflow.lib.serdes.SerializedEvent event, Map<String, Object> expected) {
    try {
      return io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER
          .readTree(event.value())
          .equals(io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER.valueToTree(expected));
    } catch (IOException failure) {
      throw new java.io.UncheckedIOException(failure);
    }
  }

  private static Map<String, Object> record(int id, Object count) {
    return Map.of("id", id, "payload", Map.of("count", count));
  }

  private static CopyIntoStats stats(int rows) {
    return new CopyIntoStats(rows, 1, 1, List.of());
  }

  private static StatementExecutionException rejection(String sqlState) {
    return new StatementExecutionException(
        "statement",
        StatementState.FAILED,
        sqlState,
        "BAD_REQUEST",
        "record rejected",
        false,
        null);
  }

  private static StatementExecutionException unknown() {
    return new StatementExecutionException(
        "statement", null, null, null, "response lost", true, null);
  }

  private static List<Integer> readIds(File file) throws IOException {
    List<Integer> ids = new ArrayList<>();
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(file.toURI()))
            .build()) {
      Group row;
      while ((row = reader.read()) != null) ids.add(row.getInteger("id", 0));
    }
    return ids;
  }

  private final class Fixture implements AutoCloseable {
    final DatabricksParquetWriter writer;
    final DatabricksVolumeUploader uploader = mock(DatabricksVolumeUploader.class);
    final DatabricksSqlExecutor sql = mock(DatabricksSqlExecutor.class);
    final DlqWriter dlq = mock(DlqWriter.class);
    final FleakCounter outputCounter = mock(FleakCounter.class);
    final FleakCounter errorCounter = mock(FleakCounter.class);
    final List<Path> attemptDirectories = new ArrayList<>();
    final Map<String, List<Integer>> uploaded = new HashMap<>();
    final Map<String, Long> uploadedBytes = new HashMap<>();
    final List<List<Integer>> copies = new ArrayList<>();
    final List<List<Integer>> validations = new ArrayList<>();
    final Set<String> validationDirectories = new HashSet<>();
    final Set<String> validatedDirectories = new HashSet<>();
    final Set<String> copyDirectories = new HashSet<>();
    final List<String> deletedDirectories = new ArrayList<>();
    final List<Integer> committed = new ArrayList<>();
    final BatchDatabricksFlusher flusher;
    long committedBytes;
    Function<List<Integer>, CopyIntoStats> copyBehavior = ids -> stats(ids.size());
    Consumer<List<Integer>> validationBehavior = ids -> {};

    Fixture(int batchSize, boolean cleanup) throws Exception {
      this(batchSize, cleanup, SCHEMA);
    }

    Fixture(int batchSize, boolean cleanup, StructType schema) throws Exception {
      writer = spy(new DatabricksParquetWriter(schema));
      Path directory = Files.createTempDirectory(temporaryDirectory, "flusher-");
      var config =
          DatabricksSinkDto.Config.builder()
              .volumePath("/Volumes/test/schema/volume")
              .tableName("test.schema.table")
              .warehouseId("warehouse")
              .batchSize(batchSize)
              .flushIntervalMillis(60000)
              .cleanupAfterCopy(cleanup)
              .build();
      flusher =
          new BatchDatabricksFlusher(
              config,
              writer,
              uploader,
              sql,
              directory,
              dlq,
              schema,
              outputCounter,
              mock(FleakCounter.class),
              errorCounter,
              "sink");
      doAnswer(
              invocation -> {
                attemptDirectories.add(invocation.getArgument(1));
                return invocation.callRealMethod();
              })
          .when(writer)
          .writeParquetFiles(anyList(), any());
      doAnswer(
              invocation -> {
                recordUpload(invocation.getArgument(0), invocation.getArgument(1));
                return null;
              })
          .when(uploader)
          .uploadFile(any(), anyString());
      doAnswer(
              invocation -> {
                deletedDirectories.add(invocation.getArgument(0));
                return null;
              })
          .when(uploader)
          .deleteDirectory(anyString());
      doAnswer(
              invocation -> {
                String pattern = invocation.getArgument(1);
                String directoryPath = pattern.substring(0, pattern.lastIndexOf('/'));
                assertTrue(validationDirectories.add(directoryPath), "Each attempt validates once");
                List<Integer> ids = List.copyOf(uploaded.get(directoryPath));
                validations.add(ids);
                validationBehavior.accept(ids);
                validatedDirectories.add(directoryPath);
                return null;
              })
          .when(sql)
          .validateCopyInto(anyString(), anyString(), anyMap(), anyMap());
      when(sql.executeCopyIntoWithStats(anyString(), anyString(), anyMap(), anyMap()))
          .thenAnswer(
              invocation -> {
                String pattern = invocation.getArgument(1);
                String directoryPath = pattern.substring(0, pattern.lastIndexOf('/'));
                assertTrue(
                    validatedDirectories.contains(directoryPath), "Validate before every mutation");
                assertTrue(copyDirectories.add(directoryPath), "Never replay a COPY attempt");
                List<Integer> ids = List.copyOf(uploaded.get(directoryPath));
                copies.add(ids);
                CopyIntoStats result = copyBehavior.apply(ids);
                committed.addAll(ids);
                committedBytes += uploadedBytes.get(directoryPath);
                return result;
              });
    }

    void recordUpload(File file, String path) throws IOException {
      String directoryPath = path.substring(0, path.lastIndexOf('/'));
      uploaded.computeIfAbsent(directoryPath, ignored -> new ArrayList<>()).addAll(readIds(file));
      uploadedBytes.merge(directoryPath, Files.size(file.toPath()), Long::sum);
    }

    SimpleSinkCommand.FlushResult flush(List<Map<String, Object>> records) throws Exception {
      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      for (Map<String, Object> record : records)
        events.add((RecordFleakData) FleakData.wrap(record), record);
      return flusher.flush(events, Map.of());
    }

    @Override
    public void close() throws Exception {
      flusher.close();
    }
  }
}
