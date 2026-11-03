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
package io.fleak.zephflow.lib.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.io.File;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import org.apache.parquet.example.data.Group;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ColumnarBatchParquetWriterTest {

  private static final StructType SCHEMA =
      new StructType().add("id", LongType.LONG, false).add("name", StringType.STRING, true);

  @TempDir Path tempDir;

  @Test
  void startsANewFileOnceTheTargetSizeIsReached() throws Exception {
    List<Map<String, Object>> rows =
        LongStream.range(0, 5_000)
            .mapToObj(i -> Map.<String, Object>of("id", i, "name", "row-" + i))
            .toList();

    List<File> files =
        new ColumnarBatchParquetWriter(SCHEMA, 1024)
            .write(
                Lists.partition(rows, 1000).stream()
                    .map(chunk -> ColumnarBatchConverter.convertSingleBatch(chunk, SCHEMA))
                    .iterator(),
                tempDir);

    assertTrue(files.size() > 1, "expected several files, got " + files.size());
    long expectedId = 0;
    for (File file : files) {
      List<Group> groups = ParquetTestReader.readGroups(List.of(file));
      assertTrue(!groups.isEmpty(), file + " is empty");
      for (Group group : groups) {
        assertEquals(expectedId, group.getLong("id", 0));
        assertEquals("row-" + expectedId, group.getString("name", 0));
        expectedId++;
      }
    }
    assertEquals(rows.size(), expectedId);
  }

  @Test
  void createsAMissingDirectory() throws Exception {
    Path missing = tempDir.resolve("not/yet/there");
    List<File> files =
        new ColumnarBatchParquetWriter(SCHEMA)
            .write(
                List.of(
                        ColumnarBatchConverter.convertSingleBatch(
                            List.of(Map.<String, Object>of("id", 7L, "name", "seven")), SCHEMA))
                    .iterator(),
                missing);

    assertEquals(1, files.size());
    List<Group> groups = ParquetTestReader.readGroups(files);
    assertEquals(7L, groups.get(0).getLong("id", 0));
  }

  @Test
  void failureMidWriteClosesTheOpenFileAndRethrows() throws Exception {
    RuntimeException failure = new RuntimeException("source failed");
    Iterator<FilteredColumnarBatch> batches =
        new Iterator<>() {
          private boolean served;

          @Override
          public boolean hasNext() {
            return true;
          }

          @Override
          public FilteredColumnarBatch next() {
            if (served) {
              throw failure;
            }
            served = true;
            return ColumnarBatchConverter.convertSingleBatch(
                List.of(Map.<String, Object>of("id", 1L, "name", "one")), SCHEMA);
          }
        };

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> new ColumnarBatchParquetWriter(SCHEMA).write(batches, tempDir));

    assertSame(failure, thrown);
    File[] written = tempDir.toFile().listFiles((dir, name) -> name.endsWith(".parquet"));
    assertEquals(1, written.length);
    List<Group> groups = ParquetTestReader.readGroups(List.of(written[0]));
    assertEquals(1L, groups.get(0).getLong("id", 0));
  }

  @Test
  void rejectsABatchWithAnotherSchema() {
    StructType other = new StructType().add("id", LongType.LONG, false);
    FilteredColumnarBatch batch =
        ColumnarBatchConverter.convertSingleBatch(List.of(Map.<String, Object>of("id", 1L)), other);

    assertThrows(
        IllegalArgumentException.class,
        () -> new ColumnarBatchParquetWriter(SCHEMA).write(List.of(batch).iterator(), tempDir));
  }

  @Test
  void noRowsWriteNoFiles() throws Exception {
    List<File> files =
        new ColumnarBatchParquetWriter(SCHEMA)
            .write(List.<FilteredColumnarBatch>of().iterator(), tempDir);
    assertEquals(List.of(), files);
  }
}
