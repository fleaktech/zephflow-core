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
package io.fleak.zephflow.lib.serdes.decode;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.io.input.BoundedInputStream;

/**
 * The bytes to decode. Implemented by the caller; the decoder only calls it and does not own its
 * lifecycle. An implementation that knows the expected length must throw an {@link IOException} on
 * a premature end instead of returning -1, because formats without an end marker cannot detect
 * truncation themselves. Formats that start from a footer (parquet) read through {@link
 * #randomAccess()} instead.
 */
@FunctionalInterface
public interface ByteInput {

  /** The raw (still compressed) bytes. Called at most once per cursor. */
  InputStream openStream() throws IOException;

  /**
   * Random access to the same bytes, or empty when the input only streams. Must not read; the reads
   * happen through the returned object.
   */
  default Optional<RandomAccessInput> randomAccess() {
    return Optional.empty();
  }

  /** Streams the bytes, and offers random access to them. */
  static ByteInput ofBytes(byte[] bytes) {
    Objects.requireNonNull(bytes, "bytes");
    RandomAccessInput ranges =
        new RandomAccessInput() {
          @Override
          public long length() {
            return bytes.length;
          }

          @Override
          public InputStream read(long offset, long length) {
            Objects.checkFromIndexSize(offset, length, bytes.length);
            return new ByteArrayInputStream(bytes, (int) offset, (int) length);
          }
        };
    return new ByteInput() {
      @Override
      public InputStream openStream() {
        return new ByteArrayInputStream(bytes);
      }

      @Override
      public Optional<RandomAccessInput> randomAccess() {
        return Optional.of(ranges);
      }
    };
  }

  /** Streams a local file, and offers random access to it. The file must not change while read. */
  static ByteInput ofFile(Path file) {
    Objects.requireNonNull(file, "file");
    RandomAccessInput ranges =
        new RandomAccessInput() {
          @Override
          public long length() throws IOException {
            return Files.size(file);
          }

          @Override
          public InputStream read(long offset, long length) throws IOException {
            if (length == 0) {
              return InputStream.nullInputStream();
            }
            FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
            try {
              channel.position(offset);
            } catch (IOException | RuntimeException e) {
              channel.close();
              throw e;
            }
            return BoundedInputStream.builder()
                .setInputStream(Channels.newInputStream(channel))
                .setMaxCount(length)
                .get();
          }
        };
    return new ByteInput() {
      @Override
      public InputStream openStream() throws IOException {
        return new BufferedInputStream(Files.newInputStream(file));
      }

      @Override
      public Optional<RandomAccessInput> randomAccess() {
        return Optional.of(ranges);
      }
    };
  }
}
