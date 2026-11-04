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
package io.fleak.zephflow.lib.serdes.decode.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Refuses compressed input for a format that would otherwise read its bytes as characters and emit
 * them as records. Only codecs with a signature that text does not start with are recognized, so
 * deflate and raw blocks pass through.
 */
public final class UndeclaredCompression {

  private static final int ANY = -1;

  private record Signature(String codec, int... bytes) {
    boolean couldStartWith(byte[] head) {
      return head.length < bytes.length && agrees(head, head.length);
    }

    boolean matches(byte[] head) {
      return head.length >= bytes.length && agrees(head, bytes.length);
    }

    private boolean agrees(byte[] head, int length) {
      for (int i = 0; i < length; i++) {
        if (bytes[i] != ANY && bytes[i] != (head[i] & 0xFF)) {
          return false;
        }
      }
      return true;
    }
  }

  private static final List<Signature> SIGNATURES =
      List.of(
          new Signature("gzip", 0x1f, 0x8b, 0x08),
          new Signature("zstd", 0x28, 0xb5, 0x2f, 0xfd),
          new Signature("xz", 0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00),
          new Signature("bzip2", 0x42, 0x5a, 0x68, ANY, 0x31, 0x41, 0x59, 0x26, 0x53, 0x59),
          new Signature("lz4", 0x04, 0x22, 0x4d, 0x18),
          new Signature("snappy", 0xff, 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59));

  private static final int LONGEST =
      SIGNATURES.stream().mapToInt(s -> s.bytes().length).max().orElseThrow();

  private UndeclaredCompression() {}

  public static FormatFactory refusing(FormatFactory format) {
    return (in, limits, flags) -> format.create(checked(in), limits, flags);
  }

  private static InputStream checked(InputStream in) {
    try {
      PeekedStream peeked =
          PeekedStream.peek(
              in, LONGEST, head -> SIGNATURES.stream().noneMatch(s -> s.couldStartWith(head)));
      byte[] head = peeked.head();
      SIGNATURES.stream()
          .filter(s -> s.matches(head))
          .findFirst()
          .ifPresent(
              s -> {
                throw new UndeclaredCompressionException(s.codec());
              });
      return peeked;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
