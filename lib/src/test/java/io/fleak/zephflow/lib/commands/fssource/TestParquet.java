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
package io.fleak.zephflow.lib.commands.fssource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;

/** Parquet files for FsSource tests, written by parquet-mr's example writer. */
final class TestParquet {

  private TestParquet() {}

  static byte[] write(MessageType schema, List<Group> rows) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new StreamOutputFile(bytes))
            .withType(schema)
            .withConf(new Configuration(false))
            .build()) {
      for (Group row : rows) {
        writer.write(row);
      }
    }
    return bytes.toByteArray();
  }

  private record StreamOutputFile(OutputStream out) implements OutputFile {
    @Override
    public PositionOutputStream create(long blockSizeHint) {
      return new PositionOutputStream() {
        private long pos;

        @Override
        public long getPos() {
          return pos;
        }

        @Override
        public void write(int b) throws IOException {
          out.write(b);
          pos++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          out.write(b, off, len);
          pos += len;
        }
      };
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
      return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return 0;
    }
  }
}
