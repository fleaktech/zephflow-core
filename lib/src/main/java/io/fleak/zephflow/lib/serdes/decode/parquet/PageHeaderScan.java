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
package io.fleak.zephflow.lib.serdes.decode.parquet;

import org.apache.parquet.format.PageHeader;

/**
 * Reads a column chunk's page headers before parquet's reader does, with {@link BoundedThrift}:
 * parquet parses them with an unbounded protocol, and reads the last chunk of a row group into a
 * buffer of whatever size its page claims. A page must lie within the file's data, before the
 * footer, and claim at most the budget compressed and uncompressed; the headers must reach the
 * chunk's value count within the chunk, as parquet's reader walks them. Page contents are not
 * checked.
 */
final class PageHeaderScan {

  private PageHeaderScan() {}

  /**
   * The chunk at {@code offset} in {@code bytes}, {@code length} bytes long and starting at {@code
   * position} in the file, whose data ends at {@code dataEnd}.
   */
  static void check(
      byte[] bytes,
      int offset,
      int length,
      long position,
      long dataEnd,
      long valueCount,
      long budget,
      String column)
      throws MalformedParquetException {
    int end = offset + length;
    int pos = offset;
    long values = 0;
    while (values < valueCount) {
      if (pos >= end) {
        throw new MalformedParquetException(
            "parquet column chunk "
                + column
                + " ends after "
                + values
                + " of "
                + valueCount
                + " values");
      }
      BoundedThrift.Parsed<PageHeader> read = BoundedThrift.pageHeader(bytes, pos, end);
      PageHeader header = read.value();
      pos += read.length();
      long compressed = header.getCompressed_page_size();
      long uncompressed = header.getUncompressed_page_size();
      long dataStart = position + (pos - offset);
      if (compressed < 0
          || uncompressed < 0
          || compressed > budget
          || uncompressed > budget
          || compressed > dataEnd - dataStart) {
        throw new MalformedParquetException(
            "parquet page in column chunk "
                + column
                + " claims "
                + compressed
                + " bytes compressed and "
                + uncompressed
                + " uncompressed, more than the file or the limit allows");
      }
      int pageValues =
          header.isSetData_page_header()
              ? header.getData_page_header().getNum_values()
              : header.isSetData_page_header_v2()
                  ? header.getData_page_header_v2().getNum_values()
                  : 0;
      if (pageValues < 0) {
        throw new MalformedParquetException(
            "parquet page in column chunk " + column + " claims " + pageValues + " values");
      }
      values += pageValues;
      pos += (int) Math.min(compressed, Integer.MAX_VALUE - pos);
    }
  }
}
