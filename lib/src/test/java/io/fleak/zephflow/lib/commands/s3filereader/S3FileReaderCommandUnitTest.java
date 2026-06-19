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
package io.fleak.zephflow.lib.commands.s3filereader;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.CommandType;
import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import org.junit.jupiter.api.Test;

class S3FileReaderCommandUnitTest {

  @Test
  void parsesPlainPath() {
    var p = S3FileReaderCommand.parseS3Path("s3://my-bucket/a/b/c.log.gz", true);
    assertEquals("my-bucket", p.bucket());
    assertEquals("a/b/c.log.gz", p.key());
  }

  @Test
  void urlDecodesKeyOnly() {
    var p = S3FileReaderCommand.parseS3Path("s3://my-bucket/2026/03/19/foo+bar%2Fx.log.gz", true);
    assertEquals("my-bucket", p.bucket());
    assertEquals("2026/03/19/foo bar/x.log.gz", p.key());
  }

  @Test
  void skipsUrlDecodeWhenDisabled() {
    var p = S3FileReaderCommand.parseS3Path("s3://my-bucket/foo+bar.log", false);
    assertEquals("foo+bar.log", p.key());
  }

  @Test
  void rejectsNonS3Path() {
    assertThrows(
        IllegalArgumentException.class,
        () -> S3FileReaderCommand.parseS3Path("https://example.com/x", true));
  }

  @Test
  void rejectsPathWithoutKey() {
    assertThrows(
        IllegalArgumentException.class,
        () -> S3FileReaderCommand.parseS3Path("s3://only-bucket", true));
  }

  @Test
  void gunzipDecision() {
    assertTrue(S3FileReaderCommand.shouldGunzip(S3FileReaderDto.Compression.GZIP, "x.log"));
    assertFalse(S3FileReaderCommand.shouldGunzip(S3FileReaderDto.Compression.NONE, "x.log.gz"));
    assertTrue(S3FileReaderCommand.shouldGunzip(S3FileReaderDto.Compression.AUTO, "x.log.gz"));
    assertFalse(S3FileReaderCommand.shouldGunzip(S3FileReaderDto.Compression.AUTO, "x.log"));
  }

  @Test
  void registeredAsIntermediateCommand() {
    var factory = OperatorCommandRegistry.OPERATOR_COMMANDS.get("s3filereader");
    assertNotNull(factory);
    assertEquals(CommandType.INTERMEDIATE_COMMAND, factory.commandType());
  }
}
