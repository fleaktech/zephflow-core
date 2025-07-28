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
package io.fleak.zephflow.lib.commands.reader;

import static io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredential;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.sql.exec.utils.Streams;
import io.fleak.zephflow.lib.utils.StreamUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

@Slf4j
public class ReaderFetcher implements Fetcher<SerializedEvent> {

  private final Path path;
  private final Configuration hadoopConf;
  private FileSystem fs;
  private final int batchSize;

  private Iterator<List<SerializedEvent>> eventsIterator;

  public ReaderFetcher(JobContext context, ReaderDto.Config config) {
    batchSize = config.getBatchSize();
    this.path =
        new Path(
            Objects.requireNonNull(
                StringUtils.trimToNull(config.getPath()), "tablePath must be set"));

    hadoopConf = new Configuration();
    if (StringUtils.trimToNull(config.getCredentialId()) != null) {
      var usernamePasswordCredential =
          lookupUsernamePasswordCredential(context, config.getCredentialId());
      hadoopConf.set("spark.hadoop.fs.s3a.access.key", usernamePasswordCredential.getUsername());
      hadoopConf.set("spark.hadoop.fs.s3a.secret.key", usernamePasswordCredential.getPassword());
    }

    config
        .getConfig()
        .forEach(
            (k, v) -> {
              if (v != null) {
                String cleanKey =
                    k.startsWith("spark.hadoop.") ? k.substring("spark.hadoop.".length()) : k;
                hadoopConf.set(cleanKey, v.toString());
              }
            });
  }

  @SneakyThrows
  public void open() {
    if (fs == null) {
      fs = FileSystem.get(path.toUri(), hadoopConf);
    }
  }

  @SneakyThrows
  public void close() {
    if (fs != null) {
      fs.close();
      fs = null;
    }
  }

  @Override
  public List<SerializedEvent> fetch() {
    open();
    if (eventsIterator == null) {
      // for each file, we read it and return the records as java maps
      var records = getFiles(fs).flatMap(s -> streamFile(fs, s));
      eventsIterator = StreamUtils.partition(records, batchSize).iterator();
    }

    if (eventsIterator.hasNext()) {
      return eventsIterator.next();
    }
    return Collections.emptyList();
  }

  @SneakyThrows
  private Stream<Path> getFiles(FileSystem fs) {
    if (fs.getFileStatus(path).isFile()) {
      return Stream.of(path);
    }
    // fetch the files lazily
    final var remoteIterator = fs.listFiles(path, true);

    // HDFS iterator is not a java iterator.
    return Streams.asStream(
        new Iterator<>() {
          @SneakyThrows
          @Override
          public boolean hasNext() {
            return remoteIterator.hasNext();
          }

          @SneakyThrows
          @Override
          public Path next() {
            return remoteIterator.next().getPath();
          }
        });
  }

  @SneakyThrows
  private Stream<SerializedEvent> streamFile(FileSystem fs, Path file) {
    // Only supports text formats i.e. csv, json
    // gz, bzip2, snappy compression is handled transparently

    var factory = new CompressionCodecFactory(fs.getConf());
    var codec = factory.getCodec(file);

    InputStream in = fs.open(file);
    if (codec != null) {
      in = codec.createInputStream(in);
    }

    var reader = new BufferedReader(new InputStreamReader(in));
    return reader
        .lines()
        .onClose(
            () -> {
              try {
                reader.close();
              } catch (IOException e) {
                log.warn("error closing stream: {}", file, e);
              }
            })
        .filter(StringUtils::isNotEmpty)
        .map(l -> new SerializedEvent(null, l.getBytes(StandardCharsets.UTF_8), null));
  }

  @Override
  public Committer commiter() {
    return () -> {};
  }
}
