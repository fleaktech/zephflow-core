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
package io.fleak.zephflow.lib.serdes.decode.auto;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatCursor;
import io.fleak.zephflow.lib.serdes.decode.internal.PeekedStream;
import io.fleak.zephflow.lib.serdes.decode.internal.WholeInputReader;
import io.fleak.zephflow.lib.serdes.decode.json.JsonEncoding;
import io.fleak.zephflow.lib.serdes.decode.json.JsonRecognition;
import io.fleak.zephflow.lib.serdes.decode.yaml.YamlRecognition;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class AutoFormatCursor implements FormatCursor {
  public enum Unit {
    MESSAGE,
    FILE
  }

  private final InputStream input;
  private final DecodeLimits limits;
  private final DecodeFlags flags;
  private final Unit unit;
  private RecognitionPrefix prefix;
  private AutoRecordFramer framer;
  private boolean started;
  private boolean done;
  private long index;
  private long offset;
  private long normalizedEnd = -1;

  public AutoFormatCursor(InputStream input, DecodeLimits limits, DecodeFlags flags, Unit unit) {
    this.input = input;
    this.limits = limits;
    this.flags = flags;
    this.unit = unit;
  }

  @Override
  public DecodeOutcome next() throws IOException {
    if (done) return null;
    if (Thread.currentThread().isInterrupted()) {
      var failure = new InterruptedIOException("Format decoding interrupted");
      flags.markRead(failure);
      throw failure;
    }
    if (!started) {
      started = true;
      PeekedStream peeked = PeekedStream.peek(input, 8, head -> head.length == 8);
      rejectUnsupported(peeked.head());
      InputStream text = JsonEncoding.strictUtf8(peeked);
      if (unit == Unit.MESSAGE) {
        done = true;
        return switch (WholeInputReader.read(text, limits.maxDocumentBytes())) {
          case WholeInputReader.TooLarge large -> large.outcome();
          case WholeInputReader.Whole whole -> record(whole.bytes(), 0);
        };
      }
      prefix = new RecognitionPrefix(text, limits.maxDocumentBytes(), flags);
      if (jsonDocument()) {
        done = true;
        return record(prefix.whole(), 0);
      }
      int yamlDocuments =
          YamlRecognition.candidate(prefix.probe())
              ? YamlRecognition.documentCount(prefix.probe(), limits.maxDocumentBytes())
              : 0;
      if (flags.anySet()) throw new IOException("Input failed during format recognition");
      if (yamlDocuments == 1) {
        done = true;
        return record(prefix.whole(), 0);
      }
      framer = new AutoRecordFramer(prefix.replay(), limits, flags, yamlDocuments > 1);
    }
    AutoRecordFramer.Frame frame = framer.next();
    if (frame == null) {
      done = true;
      return null;
    }
    offset = frame.offset();
    normalizedEnd = framer.consumedEnd();
    if (frame.oversized()) {
      return new DecodeOutcome.Oversized(
          ++index,
          1,
          new RawSpan(offset, frame.length()),
          frame.kind(),
          frame.limit(),
          frame.length(),
          new byte[0],
          false);
    }
    return record(frame.bytes(), frame.offset());
  }

  private boolean jsonDocument() throws IOException {
    try (InputStream probe = prefix.probe()) {
      int first;
      do {
        first = probe.read();
      } while (first == ' ' || first == '\t' || first == '\r' || first == '\n');
      if (first == 0xEF) {
        if (probe.read() != 0xBB || probe.read() != 0xBF) return false;
      } else if (first != '{' && first != '[') return false;
    }
    try (JsonParser parser = JsonRecognition.parser(prefix.probe())) {
      JsonToken token = parser.nextToken();
      if (token != JsonToken.START_OBJECT && token != JsonToken.START_ARRAY) return false;
      parser.skipChildren();
      return parser.nextToken() == null;
    } catch (JsonProcessingException exception) {
      if (flags.anySet()) throw exception;
      return false;
    }
  }

  private DecodeOutcome record(byte[] bytes, long start) {
    normalizedEnd = Math.max(normalizedEnd, start + bytes.length);
    index++;
    offset = start;
    try {
      String text =
          StandardCharsets.UTF_8
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT)
              .decode(ByteBuffer.wrap(bytes))
              .toString();
      return new DecodeOutcome.Decoded(
          FormatDetector.classify(text), index, new RawSpan(start, bytes.length));
    } catch (CharacterCodingException exception) {
      return new DecodeOutcome.Failed(
          ErrorKind.MALFORMED, bytes, index, new RawSpan(start, bytes.length), exception, false);
    }
  }

  private static void rejectUnsupported(byte[] head) throws UnsupportedFormatException {
    byte[][] signatures = {
      {'O', 'b', 'j', 1},
      {'P', 'A', 'R', '1'},
      {'P', 'K', 3, 4},
      {(byte) 0x89, 'P', 'N', 'G'},
      {'%', 'P', 'D', 'F'}
    };
    for (byte[] signature : signatures) {
      if (head.length >= signature.length
          && Arrays.equals(signature, Arrays.copyOf(head, signature.length))) {
        throw new UnsupportedFormatException(
            "Binary format is not supported by automatic text recognition");
      }
    }
  }

  @Override
  public long pendingRecordIndex() {
    return index + 1;
  }

  @Override
  public long pendingOffset() {
    return offset;
  }

  @Override
  public long normalizedEndOffset() {
    return normalizedEnd;
  }

  @Override
  public void close() {
    done = true;
    Exception failure = null;
    for (AutoCloseable resource : new AutoCloseable[] {prefix, input}) {
      if (resource == null) continue;
      try {
        resource.close();
      } catch (Exception exception) {
        if (failure == null) failure = exception;
        else failure.addSuppressed(exception);
      }
    }
    if (failure != null)
      throw new IllegalStateException("Closing format recognition resources failed", failure);
  }
}
