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
package org.apache.parquet;

import java.util.Map;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.converter.ParquetMetadataConverter;

/**
 * Read options that never touch Hadoop. Lives in parquet's package because both {@link
 * ParquetReadOptions.Builder} constructors create Hadoop codecs; only the package-private
 * constructor avoids them.
 */
public final class HadoopFreeReadOptions {

  private static final int MAX_ALLOCATION_SIZE = 8 * 1024 * 1024;

  private HadoopFreeReadOptions() {}

  /** Filters, page checksums and vectored IO off; pages decompress through {@code codecs}. */
  public static ParquetReadOptions create(CompressionCodecFactory codecs) {
    return new ParquetReadOptions(
        false, // useSignedStringMinMax
        false, // useStatsFilter
        false, // useDictionaryFilter
        false, // useRecordFilter
        false, // useColumnIndexFilter
        false, // usePageChecksumVerification
        false, // useBloomFilter
        false, // useOffHeapDecryptBuffer
        false, // useHadoopVectoredIo
        FilterCompat.NOOP,
        ParquetMetadataConverter.NO_FILTER,
        codecs,
        new HeapByteBufferAllocator(),
        MAX_ALLOCATION_SIZE,
        Map.of(),
        null,
        null,
        new PlainParquetConfiguration());
  }
}
