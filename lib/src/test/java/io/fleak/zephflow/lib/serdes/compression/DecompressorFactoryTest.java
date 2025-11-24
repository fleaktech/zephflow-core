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
package io.fleak.zephflow.lib.serdes.compression;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.serdes.CompressionType;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class DecompressorFactoryTest {

  @Test
  public void testGetDecompressorWithNullType() {
    var decompressor = DecompressorFactory.getDecompressor((CompressionType) null);
    assertTrue(
        decompressor instanceof NoopDecompressor, "Expected NoopDecompressor for null input");
  }

  @Test
  public void testGetDecompressorWithGzip() {
    var decompressor = DecompressorFactory.getDecompressor(CompressionType.GZIP);
    assertTrue(
        decompressor instanceof GzipDecompressor, "Expected GzipDecompressor for GZIP input");
  }

  @Test
  public void testGetDecompressorWithNullList() {
    var decompressor = DecompressorFactory.getDecompressor((List<CompressionType>) null);
    assertTrue(
        decompressor instanceof NoopDecompressor, "Expected NoopDecompressor for null list input");
  }

  @Test
  public void testGetDecompressorWithEmptyList() {
    var decompressor = DecompressorFactory.getDecompressor(Collections.emptyList());
    assertTrue(
        decompressor instanceof NoopDecompressor, "Expected NoopDecompressor for empty list input");
  }

  @Test
  public void testGetDecompressorWithSingleTypeList() {
    var decompressor = DecompressorFactory.getDecompressor(List.of(CompressionType.GZIP));
    assertTrue(
        decompressor instanceof CompositeDecompressor,
        "Expected CompositeDecompressor for list input");
  }

  @Test
  public void testGetDecompressorWithMultipleTypes() {
    var decompressor =
        DecompressorFactory.getDecompressor(List.of(CompressionType.GZIP, CompressionType.GZIP));
    assertTrue(
        decompressor instanceof CompositeDecompressor,
        "Expected CompositeDecompressor for multiple compression types");
  }
}
