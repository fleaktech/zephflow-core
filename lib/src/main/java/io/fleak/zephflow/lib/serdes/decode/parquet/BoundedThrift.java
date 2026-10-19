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
import java.nio.ByteBuffer;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageHeader;
import shaded.parquet.org.apache.thrift.TBase;
import shaded.parquet.org.apache.thrift.TConfiguration;
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.protocol.TList;
import shaded.parquet.org.apache.thrift.protocol.TMap;
import shaded.parquet.org.apache.thrift.protocol.TProtocolException;
import shaded.parquet.org.apache.thrift.protocol.TSet;
import shaded.parquet.org.apache.thrift.protocol.TStruct;
import shaded.parquet.org.apache.thrift.transport.TTransport;
import shaded.parquet.org.apache.thrift.transport.TTransportException;

/**
 * Thrift structures read only from the bytes at hand: no string, binary or container may claim more
 * bytes or elements than bytes are left (every element takes at least one), and structures nest at
 * most {@value #MAX_DEPTH} deep. parquet's own reader allocates whatever a length or count says.
 * The objects built are estimated as they are read ({@value #STRUCT_BYTES} bytes per structure,
 * {@value #CONTAINER_BYTES} per container and {@value #ELEMENT_BYTES} per element, {@value
 * #STRING_BYTES} per string, plus the bytes read), so a footer stops once it would take more than
 * its budget.
 */
final class BoundedThrift {

  static final int MAX_DEPTH = 64;
  private static final long STRUCT_BYTES = 128;
  private static final long CONTAINER_BYTES = 40;
  private static final long ELEMENT_BYTES = 16;
  private static final long STRING_BYTES = 64;

  private BoundedThrift() {}

  record Parsed<T>(T value, int length, long heap) {}

  static Parsed<FileMetaData> fileMetaData(byte[] bytes, long budget)
      throws MalformedParquetException, TooLargeException {
    return read(new FileMetaData(), bytes, 0, bytes.length, budget);
  }

  /** The page header at {@code offset}, and how many bytes it took. */
  static Parsed<PageHeader> pageHeader(byte[] bytes, int offset, int end)
      throws MalformedParquetException {
    try {
      return read(new PageHeader(), bytes, offset, end - offset, Long.MAX_VALUE);
    } catch (TooLargeException e) {
      throw new AssertionError("an unlimited budget was exceeded", e);
    }
  }

  private static <T extends TBase<?, ?>> Parsed<T> read(
      T target, byte[] bytes, int offset, int length, long budget)
      throws MalformedParquetException, TooLargeException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes, offset, length);
    Protocol protocol;
    try {
      protocol = new Protocol(new BytesLeft(in), in, length, budget);
      target.read(protocol);
    } catch (OverBudget e) {
      throw new TooLargeException("footer", e.heap, budget);
    } catch (TException | RuntimeException e) {
      throw new MalformedParquetException(
          "parquet " + target.getClass().getSimpleName() + " does not parse: " + e.getMessage(), e);
    }
    int read = length - in.available();
    return new Parsed<>(target, read, protocol.heap + read);
  }

  /**
   * Reads the bytes at hand. Thrift asks it before allocating a string or binary, so no length may
   * be more than the bytes left.
   */
  private static final class BytesLeft extends TTransport {
    private final ByteArrayInputStream in;

    BytesLeft(ByteArrayInputStream in) {
      this.in = in;
    }

    @Override
    public void checkReadBytesAvailable(long count) throws TTransportException {
      if (count > in.available()) {
        throw new TTransportException(
            TTransportException.END_OF_FILE,
            "a field claims " + count + " bytes with " + in.available() + " left");
      }
    }

    @Override
    public int read(byte[] buf, int off, int len) throws TTransportException {
      int read = in.read(buf, off, len);
      if (read < 0) {
        throw new TTransportException(TTransportException.END_OF_FILE, "the structure is cut off");
      }
      return read;
    }

    @Override
    public boolean isOpen() {
      return true;
    }

    @Override
    public void open() {}

    @Override
    public void close() {}

    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException {
      throw new TTransportException("read only");
    }

    @Override
    public TConfiguration getConfiguration() {
      return TConfiguration.DEFAULT;
    }

    @Override
    public void updateKnownMessageSize(long size) {}
  }

  private static final class OverBudget extends RuntimeException {
    final long heap;

    OverBudget(long heap) {
      super(null, null, false, false);
      this.heap = heap;
    }
  }

  private static final class Protocol extends TCompactProtocol {

    private final ByteArrayInputStream in;
    private final long budget;
    private int depth;
    private long heap;

    Protocol(BytesLeft transport, ByteArrayInputStream in, int length, long budget) {
      super(transport, length, length);
      this.in = in;
      this.budget = budget;
    }

    private void charge(long bytes) {
      heap += bytes;
      if (heap > budget) {
        throw new OverBudget(heap);
      }
    }

    @Override
    public TStruct readStructBegin() throws TException {
      if (++depth > MAX_DEPTH) {
        throw new TProtocolException("structures nest deeper than " + MAX_DEPTH + " levels");
      }
      charge(STRUCT_BYTES);
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

    @Override
    public String readString() throws TException {
      charge(STRING_BYTES);
      return super.readString();
    }

    @Override
    public ByteBuffer readBinary() throws TException {
      charge(STRING_BYTES);
      return super.readBinary();
    }

    private void checkElements(int count) throws TProtocolException {
      if (count > in.available()) {
        throw new TProtocolException(
            "a container claims " + count + " elements with " + in.available() + " bytes left");
      }
      charge(CONTAINER_BYTES + ELEMENT_BYTES * count);
    }
  }
}
