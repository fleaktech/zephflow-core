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
package io.fleak.zephflow.lib.serdes.decode.avro;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

/**
 * GenericDatumReader that checks a fixed's size, which comes from the (possibly untrusted) schema,
 * against the bytes left before avro allocates it. Avro wraps the decoder it is given, so the
 * bounded decoder is kept here.
 */
final class BoundedDatumReader extends GenericDatumReader<IndexedRecord> {

  private BoundedDecoder current;

  BoundedDatumReader(Schema schema) {
    super(schema);
  }

  IndexedRecord read(BoundedDecoder decoder) throws IOException {
    current = decoder;
    return read(null, decoder);
  }

  @Override
  protected Object readFixed(Object old, Schema expected, Decoder in) throws IOException {
    if (expected.getFixedSize() > current.remaining()) {
      throw new MalformedAvroException(
          "fixed of "
              + expected.getFixedSize()
              + " bytes is longer than the "
              + current.remaining()
              + " bytes left");
    }
    return super.readFixed(old, expected, in);
  }
}
