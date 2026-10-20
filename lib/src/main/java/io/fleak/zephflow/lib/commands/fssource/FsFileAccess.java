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

import com.google.common.annotations.VisibleForTesting;
import io.fleak.zephflow.lib.commands.filedecode.LocalSpool;
import io.fleak.zephflow.lib.commands.filedecode.SpoolDownloader;
import io.fleak.zephflow.lib.commands.filedecode.SpoolDownloader.AtLeast;
import io.fleak.zephflow.lib.commands.filedecode.SpoolDownloader.Download;
import io.fleak.zephflow.lib.commands.filedecode.SpoolDownloader.OutputOpener;
import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import io.fleak.zephflow.lib.serdes.decode.ByteInput;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.http.Abortable;

/**
 * Makes one listed file's bytes available to the decoder. A {@code file} backend file is read in
 * place. A remote file is copied whole into a spool directory first, so that nothing is decoded
 * while the network is involved; one over {@code localSpoolMaxBytes} is streamed from the backend
 * instead, unless its format needs random access. Files are handled one at a time, so the spool
 * holds at most one copy; it is created on the first copy and deleted on {@link #close}.
 */
@Slf4j
final class FsFileAccess implements AutoCloseable {

  static final long DEFAULT_LOCAL_SPOOL_MAX_BYTES = 1L << 30;
  private static final String SPOOL_PREFIX = "fs-spool-";
  private static final String COPY_PREFIX = "file-";

  private final FileReader reader;
  private final boolean inPlace;
  private final long localSpoolMaxBytes;
  private final Supplier<LocalSpool> spoolFactory;
  private final OutputOpener outputOpener;
  private LocalSpool spool;
  private SpoolDownloader downloader;

  /**
   * @param inPlace whether file urns are local paths, read without a copy
   */
  FsFileAccess(FileReader reader, boolean inPlace, long localSpoolMaxBytes) {
    this(
        reader,
        inPlace,
        localSpoolMaxBytes,
        () -> LocalSpool.create(SPOOL_PREFIX),
        Files::newOutputStream);
  }

  @VisibleForTesting
  FsFileAccess(
      FileReader reader,
      boolean inPlace,
      long localSpoolMaxBytes,
      Supplier<LocalSpool> spoolFactory,
      OutputOpener outputOpener) {
    this.reader = reader;
    this.inPlace = inPlace;
    this.localSpoolMaxBytes = localSpoolMaxBytes;
    this.spoolFactory = spoolFactory;
    this.outputOpener = outputOpener;
  }

  /** A file's bytes; closing releases them. */
  sealed interface FileBytes extends AutoCloseable {
    @Override
    default void close() {}
  }

  /** A local file, decoded where it is. */
  record InPlace(ByteInput input) implements FileBytes {}

  /** A copy of {@code bytes} bytes in the spool; closing deletes it. */
  record Copied(ByteInput input, Path file, long bytes) implements FileBytes {
    @Override
    public void close() {
      SpoolDownloader.deleteQuietly(file);
    }
  }

  /** Streamed from the backend; an end before the listed size is an error. */
  record Streamed(ByteInput input) implements FileBytes {}

  /** Needs random access and is over {@code localSpoolMaxBytes}, so it cannot be read. */
  record TooLarge() implements FileBytes {}

  /**
   * @param randomAccess whether the format needs random access (parquet), so the file cannot be
   *     streamed
   * @throws SourceReadException when the copy fails
   */
  FileBytes open(FileEntry entry, boolean randomAccess) throws SourceReadException {
    String urn = entry.key().urn();
    if (inPlace) {
      log.info("fs_source reading urn={} listedSizeBytes={} in place", urn, entry.size());
      return new InPlace(ByteInput.ofFile(Path.of(URI.create(urn))));
    }
    String overCap = "listed over";
    if (entry.size() <= localSpoolMaxBytes) {
      Download<Void> download =
          downloader()
              .download(
                  urn,
                  () ->
                      new AtLeast<>(
                          reader.open(entry.key(), 0), entry.size(), localSpoolMaxBytes, null));
      if (download instanceof SpoolDownloader.Copied<Void> copied) {
        log.info(
            "fs_source reading urn={} listedSizeBytes={} from a local copy {} of {} bytes",
            urn,
            entry.size(),
            copied.file(),
            copied.bytes());
        return new Copied(ByteInput.ofFile(copied.file()), copied.file(), copied.bytes());
      }
      overCap = "grew past";
    }
    if (randomAccess) {
      return new TooLarge();
    }
    log.info(
        "fs_source reading urn={} listedSizeBytes={} streamed: it {} localSpoolMaxBytes {}",
        urn,
        entry.size(),
        overCap,
        localSpoolMaxBytes);
    return new Streamed(() -> new LengthChecked(reader.open(entry.key(), 0), urn, entry.size()));
  }

  @Override
  public void close() {
    if (spool != null) {
      spool.close();
      spool = null;
      downloader = null;
    }
  }

  private SpoolDownloader downloader() {
    if (downloader == null) {
      spool = spoolFactory.get();
      downloader = new SpoolDownloader(spool.getPath(), COPY_PREFIX, outputOpener);
    }
    return downloader;
  }

  /**
   * Throws on an end before {@code length} bytes, which no backend stream does itself, and aborts
   * the connection when it is closed before its end.
   */
  private static final class LengthChecked extends FilterInputStream {
    private final String urn;
    private final long length;
    private long read;
    private boolean ended;

    LengthChecked(InputStream in, String urn, long length) {
      super(in);
      this.urn = urn;
      this.length = length;
    }

    @Override
    public int read() throws IOException {
      int b = super.read();
      count(b < 0 ? -1 : 1);
      return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int n = super.read(b, off, len);
      count(n);
      return n;
    }

    @Override
    public long skip(long n) throws IOException {
      long skipped = super.skip(n);
      read += skipped;
      return skipped;
    }

    private void count(int n) throws EOFException {
      if (n >= 0) {
        read += n;
        return;
      }
      ended = true;
      if (read < length) {
        throw new EOFException(
            "the stream of %s ended after %d of %d bytes".formatted(urn, read, length));
      }
    }

    @Override
    public void close() throws IOException {
      if (!ended && in instanceof Abortable abortable) {
        try {
          abortable.abort();
        } catch (RuntimeException e) {
          log.debug("aborting the stream of {} failed", urn, e);
        }
      }
      super.close();
    }
  }
}
