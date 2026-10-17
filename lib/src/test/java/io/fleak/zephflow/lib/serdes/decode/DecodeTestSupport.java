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

import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_RAW;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfigCompiler;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

/** Turns outcomes into plain comparable values so tests can compare whole expected lists. */
final class DecodeTestSupport {

  private DecodeTestSupport() {}

  record Ok(RecordFleakData record, long recordIndex, RawSpan span) {}

  record Bad(
      ErrorKind kind, String raw, long recordIndex, RawSpan span, String error, boolean terminal) {}

  record Big(long recordIndex, RawSpan span, long sizeSoFar, String preview, boolean terminal) {}

  record Threw(String type) {}

  static RecordDecoder decoder(String configJson) {
    try {
      return DecodingConfigCompiler.compile(
          JsonUtils.OBJECT_MAPPER.readValue(configJson, DecodingConfig.class));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static List<Object> decode(RecordDecoder decoder, ByteInput input, DecodeLimits limits) {
    List<Object> views = new ArrayList<>();
    try (RecordCursor cursor = decoder.open(input, limits)) {
      DecodeOutcome outcome;
      while ((outcome = cursor.next()) != null) {
        views.add(view(outcome));
      }
    } catch (SourceReadException e) {
      views.add(new Threw(e.getClass().getSimpleName()));
    }
    return views;
  }

  static List<Object> decode(String configJson, byte[] bytes, DecodeLimits limits) {
    return decode(decoder(configJson), ByteInput.ofBytes(bytes), limits);
  }

  static List<Object> decode(String configJson, byte[] bytes) {
    return decode(configJson, bytes, DecodeLimits.UNLIMITED);
  }

  static Object view(DecodeOutcome outcome) {
    return switch (outcome) {
      case DecodeOutcome.Decoded d -> new Ok(d.record(), d.recordIndex(), d.span());
      case DecodeOutcome.Failed f ->
          new Bad(
              f.kind(),
              new String(f.raw(), StandardCharsets.UTF_8),
              f.recordIndex(),
              f.span(),
              error(f.error()),
              f.terminal());
      case DecodeOutcome.Oversized o ->
          new Big(
              o.recordIndex(),
              o.span(),
              o.sizeSoFar(),
              new String(o.preview(), StandardCharsets.UTF_8),
              o.terminal());
    };
  }

  /** Class name and the first line of the message; later lines are parser location details. */
  static String error(Exception e) {
    String message = e.getMessage() == null ? "" : e.getMessage().lines().findFirst().orElse("");
    return e.getClass().getSimpleName() + ": " + message;
  }

  static RecordFleakData json(String json) {
    return (RecordFleakData) JsonUtils.loadFleakDataFromJsonString(json);
  }

  static RecordFleakData raw(String text) {
    return new RecordFleakData(
        Map.<String, FleakData>of(FIELD_NAME_RAW, new StringPrimitiveFleakData(text)));
  }

  static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  static byte[] gzip(String... members) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (String member : members) {
      out.writeBytes(gzipOne(utf8(member)));
    }
    return out.toByteArray();
  }

  static byte[] gzipOne(byte[] data) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (GZIPOutputStream gz = new GZIPOutputStream(bytes)) {
      gz.write(data);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return bytes.toByteArray();
  }

  static byte[] concat(byte[]... parts) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] part : parts) {
      out.writeBytes(part);
    }
    return out.toByteArray();
  }

  /** Reports nothing available and hands out at most {@code maxChunk} bytes per read. */
  static ByteInput trickle(byte[] data, int maxChunk) {
    return () ->
        new InputStream() {
          private int pos;

          @Override
          public int read() {
            return pos < data.length ? data[pos++] & 0xFF : -1;
          }

          @Override
          public int read(byte[] b, int off, int len) {
            if (pos >= data.length) {
              return -1;
            }
            int n = Math.min(Math.min(len, maxChunk), data.length - pos);
            System.arraycopy(data, pos, b, off, n);
            pos += n;
            return n;
          }

          @Override
          public int available() {
            return 0;
          }
        };
  }

  /** Delivers {@code data} but throws like a dropped connection once {@code failAt} is reached. */
  static ByteInput failingAt(byte[] data, int failAt) {
    return () ->
        new InputStream() {
          private int pos;

          @Override
          public int read() throws IOException {
            byte[] one = new byte[1];
            return read(one, 0, 1) < 0 ? -1 : one[0] & 0xFF;
          }

          @Override
          public int read(byte[] b, int off, int len) throws IOException {
            if (pos >= failAt) {
              throw new IOException("connection reset");
            }
            int n = Math.min(len, failAt - pos);
            System.arraycopy(data, pos, b, off, n);
            pos += n;
            return n;
          }
        };
  }
}
