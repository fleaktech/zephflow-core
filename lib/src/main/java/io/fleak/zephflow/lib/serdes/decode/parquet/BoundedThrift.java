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

import java.io.ByteArrayInputStream;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageHeader;
import shaded.parquet.org.apache.thrift.TBase;
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.protocol.TList;
import shaded.parquet.org.apache.thrift.protocol.TMap;
import shaded.parquet.org.apache.thrift.protocol.TProtocolException;
import shaded.parquet.org.apache.thrift.protocol.TSet;
import shaded.parquet.org.apache.thrift.protocol.TStruct;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;

/**
 * Thrift structures read only from the bytes at hand: no string or container may claim more
 * elements than bytes are left (every element takes at least one), and structures nest at most
 * {@value #MAX_DEPTH} deep. parquet's own reader allocates whatever a length or count says.
 */
final class BoundedThrift {

  static final int MAX_DEPTH = 64;

  private BoundedThrift() {}

  static FileMetaData fileMetaData(byte[] bytes) throws MalformedParquetException {
    FileMetaData metadata = new FileMetaData();
    read(metadata, bytes, 0, bytes.length);
    return metadata;
  }

  /** The page header at {@code offset}, and how many bytes it took. */
  static Header pageHeader(byte[] bytes, int offset, int end) throws MalformedParquetException {
    PageHeader header = new PageHeader();
    int length = read(header, bytes, offset, end - offset);
    return new Header(header, length);
  }

  record Header(PageHeader header, int length) {}

  private static int read(TBase<?, ?> target, byte[] bytes, int offset, int length)
      throws MalformedParquetException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes, offset, length);
    try {
      target.read(new Protocol(new TIOStreamTransport(in), in, length));
    } catch (TException | RuntimeException e) {
      throw new MalformedParquetException(
          "parquet " + target.getClass().getSimpleName() + " does not parse: " + e.getMessage(), e);
    }
    return length - in.available();
  }

  private static final class Protocol extends TCompactProtocol {

    private final ByteArrayInputStream in;
    private int depth;

    Protocol(TIOStreamTransport transport, ByteArrayInputStream in, int length) {
      super(transport, length, length);
      this.in = in;
    }

    @Override
    public TStruct readStructBegin() throws TException {
      if (++depth > MAX_DEPTH) {
        throw new TProtocolException("structures nest deeper than " + MAX_DEPTH + " levels");
      }
      return super.readStructBegin();
    }

    @Override
    public void readStructEnd() throws TException {
      depth--;
      super.readStructEnd();
    }

    @Override
    public TList readListBegin() throws TException {
      TList list = super.readListBegin();
      checkElements(list.size);
      return list;
    }

    @Override
    public TSet readSetBegin() throws TException {
      TSet set = super.readSetBegin();
      checkElements(set.size);
      return set;
    }

    @Override
    public TMap readMapBegin() throws TException {
      TMap map = super.readMapBegin();
      checkElements(map.size);
      return map;
    }

    private void checkElements(int count) throws TProtocolException {
      if (count > in.available()) {
        throw new TProtocolException(
            "a container claims " + count + " elements with " + in.available() + " bytes left");
      }
    }
  }
}
