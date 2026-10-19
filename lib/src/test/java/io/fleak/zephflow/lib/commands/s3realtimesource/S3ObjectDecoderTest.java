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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.s3realtimesource.S3ObjectDecoder.Result;
import io.fleak.zephflow.lib.commands.s3realtimesource.S3ObjectDecoder.Status;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.RecordCursor;
import io.fleak.zephflow.lib.serdes.decode.RecordDecoder;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.FramingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.TextFormatConfig;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

class S3ObjectDecoderTest {

  private static final long MIB = 1024L * 1024;
  private static final S3ObjectRef REF = new S3ObjectRef("b", "logs/k.jsonl", "us-east-1");
  private static final byte[] LINES =
      "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n".getBytes(StandardCharsets.UTF_8);

  @TempDir Path spoolDir;
  private S3Client s3;
  private final List<DecodeOutcome> seen = new ArrayList<>();

  @BeforeEach
  void setUp() {
    s3 = mock(S3Client.class);
  }

  @Test
  void decodesEveryRecordAndReportsTheResponse() throws SourceReadException {
    when(s3.getObject(any(GetObjectRequest.class))).thenReturn(body(LINES));

    Result result = jsonLines(false).decode(s3, REF, 1024, spoolDir, collect());

    assertEquals(new Result(Status.FINISHED, LINES.length, "\"e\"", "v1"), result);
    assertEquals(
        List.of(
            new DecodeOutcome.Decoded(json("{\"a\":1}"), 1, new RawSpan(0, 7)),
            new DecodeOutcome.Decoded(json("{\"a\":2}"), 2, new RawSpan(8, 7)),
            new DecodeOutcome.Decoded(json("{\"a\":3}"), 3, new RawSpan(16, 7))),
        seen);
    assertEquals(List.of(), spoolFiles());
  }

  @Test
  void addS3MetadataEnrichesDecodedRecords() throws SourceReadException {
    when(s3.getObject(any(GetObjectRequest.class))).thenReturn(body("{\"a\":1}".getBytes()));

    jsonLines(true).decode(s3, REF, 1024, spoolDir, collect());

    assertEquals(
        List.of(
            new DecodeOutcome.Decoded(
                json("{\"a\":1,\"__s3_bucket\":\"b\",\"__s3_key\":\"logs/k.jsonl\"}"),
                1,
                new RawSpan(0, 7))),
        seen);
  }

  @Test
  void aVisitorThatStopsEndsTheDecodeAndTheLocalFileIsDeleted() throws SourceReadException {
    when(s3.getObject(any(GetObjectRequest.class))).thenReturn(body(LINES));

    Result result =
        jsonLines(false)
            .decode(
                s3,
                REF,
                1024,
                spoolDir,
                outcome -> {
                  seen.add(outcome);
                  return false;
                });

    assertEquals(new Result(Status.STOPPED, LINES.length, "\"e\"", "v1"), result);
    assertEquals(List.of(new DecodeOutcome.Decoded(json("{\"a\":1}"), 1, new RawSpan(0, 7))), seen);
    assertEquals(List.of(), spoolFiles());
  }

  @Test
  void aVisitorThatThrowsStillGetsTheLocalFileDeleted() {
    when(s3.getObject(any(GetObjectRequest.class))).thenReturn(body(LINES));

    assertThrows(
        IllegalStateException.class,
        () ->
            jsonLines(false)
                .decode(
                    s3,
                    REF,
                    1024,
                    spoolDir,
                    outcome -> {
                      throw new IllegalStateException("stopped");
                    }));
    assertEquals(List.of(), spoolFiles());
  }

  @Test
  void aBodyThatBreaksOnceIsDecodedOnce() throws SourceReadException {
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenReturn(response(LINES.length, breakingAfter(10)))
        .thenReturn(body(LINES));

    Result result = jsonLines(false).decode(s3, REF, 1024, spoolDir, collect());

    assertEquals(new Result(Status.FINISHED, LINES.length, "\"e\"", "v1"), result);
    assertEquals(
        List.of(json("{\"a\":1}"), json("{\"a\":2}"), json("{\"a\":3}")),
        seen.stream().map(o -> ((DecodeOutcome.Decoded) o).record()).toList());
  }

  @Test
  void aMissingObjectDecodesNothing() throws SourceReadException {
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenThrow(NoSuchKeyException.builder().message("gone").build());

    assertEquals(
        new Result(Status.MISSING, -1, null, null),
        jsonLines(false).decode(s3, REF, 1024, spoolDir, collect()));
    assertEquals(List.of(), seen);
  }

  @Test
  void anObjectOverTheLimitDecodesNothing() throws SourceReadException {
    when(s3.getObject(any(GetObjectRequest.class))).thenReturn(body(LINES));

    assertEquals(
        new Result(Status.TOO_LARGE, LINES.length, "\"e\"", "v1"),
        jsonLines(false).decode(s3, REF, LINES.length - 1, spoolDir, collect()));
    assertEquals(List.of(), seen);
  }

  @Test
  void aFailedDownloadThrows() {
    when(s3.getObject(any(GetObjectRequest.class))).thenThrow(new IllegalStateException("denied"));

    assertThrows(
        SourceReadException.class,
        () -> jsonLines(false).decode(s3, REF, 1024, spoolDir, collect()));
  }

  @Test
  void theLimitsFollowTheHeapGiven() {
    assertEquals(
        new DecodeLimits(16 * MIB, 32 * MIB, 128 * MIB),
        new S3ObjectDecoder(new S3Decoding(EncodingType.CSV, null, null, false), 2048 * MIB)
            .limits());
  }

  @Test
  void anInvalidDecodingIsRejectedWhenTheDecoderIsBuilt() {
    DecodingConfig badRegex =
        new DecodingConfig(
            null, new TextFormatConfig(null, new FramingConfig.Multiline("(", null, null)));
    assertThrows(
        IllegalArgumentException.class,
        () -> new S3ObjectDecoder(new S3Decoding(null, null, badRegex, false), 2048 * MIB));
    DecodingConfig compressedParquet =
        JsonUtils.fromJsonString(
            "{\"compression\":\"gzip\",\"format\":{\"type\":\"parquet\"}}", DecodingConfig.class);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new S3ObjectDecoder(new S3Decoding(null, null, compressedParquet, false), 2048 * MIB));
  }

  @Test
  void aHeapUnderOneMibIsRejected() {
    assertEquals(
        "heapBytes must be at least 1048576 but was 2048",
        assertThrows(
                IllegalArgumentException.class,
                () ->
                    new S3ObjectDecoder(new S3Decoding(EncodingType.CSV, null, null, false), 2048))
            .getMessage());
  }

  @Test
  void theCursorIsClosedOnEveryOutcome() {
    List<Boolean> closed = new ArrayList<>();
    RecordDecoder real = jsonLines(false).recordDecoder();
    RecordDecoder tracking =
        (input, limits) -> {
          RecordCursor cursor = real.open(input, limits);
          return new RecordCursor() {
            @Override
            public DecodeOutcome next() throws SourceReadException {
              return cursor.next();
            }

            @Override
            public void close() {
              closed.add(true);
              cursor.close();
            }
          };
        };
    S3ObjectDecoder decoder = new S3ObjectDecoder(tracking, DecodeLimits.UNLIMITED, false);
    when(s3.getObject(any(GetObjectRequest.class))).thenAnswer(invocation -> body(LINES));

    assertDoesNotThrow(() -> decoder.decode(s3, REF, 1024, spoolDir, collect()));
    assertDoesNotThrow(() -> decoder.decode(s3, REF, 1024, spoolDir, outcome -> false));
    assertThrows(
        IllegalStateException.class,
        () ->
            decoder.decode(
                s3,
                REF,
                1024,
                spoolDir,
                outcome -> {
                  throw new IllegalStateException("stopped");
                }));

    assertEquals(List.of(true, true, true), closed);
  }

  private S3ObjectDecoder jsonLines(boolean addS3Metadata) {
    return new S3ObjectDecoder(
        new S3Decoding(EncodingType.JSON_OBJECT_LINE, null, null, addS3Metadata), 2048 * MIB);
  }

  private S3ObjectDecoder.Visitor collect() {
    return outcome -> {
      seen.add(outcome);
      return true;
    };
  }

  private List<Path> spoolFiles() {
    try (Stream<Path> files = Files.list(spoolDir)) {
      return files.toList();
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private static RecordFleakData json(String json) {
    return (RecordFleakData) JsonUtils.loadFleakDataFromJsonString(json);
  }

  private static ResponseInputStream<GetObjectResponse> body(byte[] bytes) {
    return response(bytes.length, new ByteArrayInputStream(bytes));
  }

  private static ResponseInputStream<GetObjectResponse> response(long length, InputStream in) {
    return new ResponseInputStream<>(
        GetObjectResponse.builder().contentLength(length).eTag("\"e\"").versionId("v1").build(),
        AbortableInputStream.create(in, mock(Abortable.class)));
  }

  private static InputStream breakingAfter(int bytes) {
    return new InputStream() {
      private int position;

      @Override
      public int read() throws IOException {
        if (position >= bytes) {
          throw new IOException("connection reset");
        }
        return LINES[position++] & 0xff;
      }
    };
  }
}
