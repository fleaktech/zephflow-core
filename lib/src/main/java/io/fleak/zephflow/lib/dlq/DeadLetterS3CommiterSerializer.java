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
package io.fleak.zephflow.lib.dlq;

import io.fleak.zephflow.lib.commands.s3.S3CommiterSerializer;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;

/** Created by bolei on 5/5/25 */
public class DeadLetterS3CommiterSerializer implements S3CommiterSerializer<DeadLetter> {
  @Override
  public byte[] serialize(List<DeadLetter> events) throws IOException {
    // Use Avro DataFileWriter for serialization
    SpecificDatumWriter<DeadLetter> datumWriter = new SpecificDatumWriter<>(DeadLetter.class);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try (DataFileWriter<DeadLetter> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(events.get(0).getSchema(), outputStream);
      for (DeadLetter deadLetter : events) {
        dataFileWriter.append(deadLetter);
      }
    }
    return outputStream.toByteArray();
  }
}
