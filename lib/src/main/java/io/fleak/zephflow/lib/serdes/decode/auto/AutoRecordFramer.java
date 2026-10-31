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
import io.fleak.zephflow.lib.serdes.decode.LimitKind;
import io.fleak.zephflow.lib.serdes.decode.internal.BudgetCaptureStream;
import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import io.fleak.zephflow.lib.serdes.decode.json.JsonRecognition;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;

final class AutoRecordFramer {
  record Frame(byte[] bytes, long offset, long length, long limit, LimitKind kind) {
    boolean oversized() {
      return limit > 0 && length > limit;
    }
  }

  private final InputStream input;
  private final DecodeLimits limits;
  private final DecodeFlags flags;
  private final ArrayDeque<ByteBuffer> replay = new ArrayDeque<>();
  private final boolean yamlDocuments;
  private int next = -2;
  private long position;
  private byte[] pendingYamlLine;
  private long pendingYamlOffset;
  private long pendingYamlLength;

  AutoRecordFramer(
      InputStream input, DecodeLimits limits, DecodeFlags flags, boolean yamlDocuments) {
    this.input = new BufferedInputStream(input);
    this.limits = limits;
    this.flags = flags;
    this.yamlDocuments = yamlDocuments;
  }

  Frame next() throws IOException {
    if (yamlDocuments) return yaml();
    if (peek() < 0) return null;
    long start = position;
    Retained bytes =
        new Retained(
            limits.maxRecordBytes() == 0 || limits.maxDocumentBytes() == 0
                ? 0
                : Math.max(limits.maxRecordBytes(), limits.maxDocumentBytes()));
    if (position == 0 && peek() == 0xEF) {
      bytes.add(read());
      if (peek() == 0xBB) {
        bytes.add(read());
        if (peek() == 0xBF) bytes.add(read());
      }
    }
    while (peek() == ' ' || peek() == '\t') bytes.add(read());
    if (peek() == '{' || peek() == '[') {
      unread(bytes.bytes());
      Frame document = json(start);
      if (document != null) return document;
      bytes = new Retained(limits.maxRecordBytes());
    }
    boolean quoted = false;
    boolean fieldStart = bytes.length == 0;
    boolean quoteSeen = false;
    bytes.limit(limits.maxRecordBytes());
    while (peek() >= 0) {
      int character = read();
      if (quoted) {
        if (quoteSeen) {
          if (character == '"') {
            quoteSeen = false;
            bytes.add(character);
            continue;
          }
          quoted = false;
          quoteSeen = false;
        } else {
          if (character == '"') quoteSeen = true;
          bytes.add(character);
          continue;
        }
      }
      if (character == '\n' || character == '\r') {
        if (character == '\r' && peek() == '\n') read();
        break;
      }
      if (fieldStart && character == '"') quoted = true;
      fieldStart = character == ',';
      bytes.add(character);
    }
    return bytes.frame(start, limits.maxRecordBytes(), LimitKind.MAX_RECORD_BYTES);
  }

  long consumedEnd() {
    return pendingYamlLine == null ? position : pendingYamlOffset;
  }

  private Frame json(long start) throws IOException {
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    InputStream probe =
        new InputStream() {
          @Override
          public int read() throws IOException {
            long maximum = limits.maxDocumentBytes();
            if (maximum > 0 && captured.size() >= maximum + BudgetCaptureStream.READ_AHEAD_SLACK) {
              flags.markBudget(
                  new DecodeFlags.BudgetExceeded(
                      start, LimitKind.MAX_DOCUMENT_BYTES, maximum, captured.size(), new byte[0]));
              throw new IOException("JSON document exceeds maxDocumentBytes");
            }
            int value = AutoRecordFramer.this.read();
            if (value >= 0) captured.write(value);
            return value;
          }
        };
    try (JsonParser parser = JsonRecognition.parser(probe)) {
      JsonToken token = parser.nextToken();
      if (token != JsonToken.START_OBJECT && token != JsonToken.START_ARRAY) {
        unread(captured.toByteArray());
        return null;
      }
      parser.skipChildren();
      byte[] bytes = captured.toByteArray();
      int end = Math.toIntExact(parser.currentLocation().getByteOffset());
      unread(Arrays.copyOfRange(bytes, end, bytes.length));
      int following = peek();
      if (following >= 0
          && following != ' '
          && following != '\t'
          && following != '\r'
          && following != '\n'
          && following != '{'
          && following != '[') {
        unread(Arrays.copyOf(bytes, end));
        return null;
      }
      boolean multiline = false;
      for (int index = 0; index < end; index++)
        multiline |= bytes[index] == '\n' || bytes[index] == '\r';
      long maximum = multiline ? limits.maxDocumentBytes() : limits.maxRecordBytes();
      while (peek() == ' ' || peek() == '\t') read();
      if (peek() == '\r') read();
      if (peek() == '\n') read();
      return new Frame(
          maximum > 0 && end > maximum ? new byte[0] : Arrays.copyOf(bytes, end),
          start,
          end,
          maximum,
          multiline ? LimitKind.MAX_DOCUMENT_BYTES : LimitKind.MAX_RECORD_BYTES);
    } catch (JsonProcessingException exception) {
      if (flags.anySet()) throw exception;
      unread(captured.toByteArray());
      return null;
    }
  }

  private void unread(byte[] bytes) {
    if (next >= 0) replay.addFirst(ByteBuffer.wrap(new byte[] {(byte) next}));
    next = -2;
    if (bytes.length > 0) replay.addFirst(ByteBuffer.wrap(bytes));
    position -= bytes.length;
  }

  private Frame yaml() throws IOException {
    if (pendingYamlLine == null && peek() < 0) return null;
    long start = pendingYamlLine == null ? position : pendingYamlOffset;
    Retained document = new Retained(limits.maxDocumentBytes());
    boolean begun = pendingYamlLine != null;
    if (pendingYamlLine != null) {
      document.add(pendingYamlLine);
      document.length += pendingYamlLength - pendingYamlLine.length;
      pendingYamlLine = null;
    }
    while (peek() >= 0) {
      long lineOffset = position;
      Retained line = new Retained(limits.maxDocumentBytes());
      while (peek() >= 0) {
        int character = read();
        line.add(character);
        if (character == '\n') break;
      }
      byte[] value = line.bytes();
      boolean marker =
          value.length >= 3
              && value[0] == '-'
              && value[1] == '-'
              && value[2] == '-'
              && (value.length == 3
                  || value[3] == ' '
                  || value[3] == '\t'
                  || value[3] == '\r'
                  || value[3] == '\n');
      if (marker && begun) {
        pendingYamlLine = value;
        pendingYamlOffset = lineOffset;
        pendingYamlLength = line.length;
        break;
      }
      String lineText = new String(value, StandardCharsets.UTF_8).stripLeading();
      if (lineOffset == 0 && lineText.startsWith("\uFEFF"))
        lineText = lineText.substring(1).stripLeading();
      if (!lineText.isBlank() && !lineText.startsWith("#") && !lineText.startsWith("%"))
        begun = true;
      document.add(value);
      document.length += line.length - value.length;
    }
    return document.frame(start, limits.maxDocumentBytes(), LimitKind.MAX_DOCUMENT_BYTES);
  }

  private int peek() throws IOException {
    if (Thread.currentThread().isInterrupted()) {
      var failure = new InterruptedIOException("Format framing interrupted");
      flags.markRead(failure);
      throw failure;
    }
    if (next == -2) {
      while (!replay.isEmpty() && !replay.getFirst().hasRemaining()) replay.removeFirst();
      next = replay.isEmpty() ? input.read() : replay.getFirst().get() & 255;
    }
    return next;
  }

  private int read() throws IOException {
    int value = peek();
    next = -2;
    if (value >= 0) position++;
    return value;
  }

  private static final class Retained {
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private long limit;
    private long length;

    private Retained(long limit) {
      this.limit = limit;
    }

    private void limit(long limit) {
      this.limit = limit;
    }

    private void add(int value) {
      length++;
      if (limit == 0 || length <= limit) output.write(value);
    }

    private void add(byte[] bytes) {
      for (byte value : bytes) add(value & 255);
    }

    private byte[] bytes() {
      return output.toByteArray();
    }

    private Frame frame(long start, long maximum, LimitKind kind) {
      return new Frame(
          maximum > 0 && length > maximum ? new byte[0] : bytes(), start, length, maximum, kind);
    }
  }
}
