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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import org.junit.jupiter.api.Test;

class FsSourceConfigValidatorTest {

  private FsSourceDto.Config cfg() {
    return FsSourceDto.Config.builder()
        .backend("file")
        .root("file:///tmp/data")
        .fileNameRegex("invoice_(?<ts>\\d+)\\.json")
        .encodingType(EncodingType.JSON_OBJECT_LINE)
        .build();
  }

  @Test
  void acceptsValidConfig() {
    new FsSourceConfigValidator().validateConfig(cfg(), "n", JobContext.builder().build());
  }

  @Test
  void rejectsMissingBackend() {
    FsSourceDto.Config c = cfg();
    c.setBackend(null);
    assertThrows(
        IllegalArgumentException.class,
        () -> new FsSourceConfigValidator().validateConfig(c, "n", JobContext.builder().build()));
  }

  @Test
  void rejectsMissingRoot() {
    FsSourceDto.Config c = cfg();
    c.setRoot(null);
    assertThrows(
        IllegalArgumentException.class,
        () -> new FsSourceConfigValidator().validateConfig(c, "n", JobContext.builder().build()));
  }

  @Test
  void rejectsInvalidRegex() {
    FsSourceDto.Config c = cfg();
    c.setFileNameRegex("invoice_(?<ts>\\d+");
    assertThrows(
        IllegalArgumentException.class,
        () -> new FsSourceConfigValidator().validateConfig(c, "n", JobContext.builder().build()));
  }

  @Test
  void rejectsMissingEncodingType() {
    FsSourceDto.Config c = cfg();
    c.setEncodingType(null);
    assertThrows(
        IllegalArgumentException.class,
        () -> new FsSourceConfigValidator().validateConfig(c, "n", JobContext.builder().build()));
  }
}
