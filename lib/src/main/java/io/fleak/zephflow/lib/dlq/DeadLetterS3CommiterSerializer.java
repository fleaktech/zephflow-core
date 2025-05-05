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
      dataFileWriter.create(events.getFirst().getSchema(), outputStream);
      for (DeadLetter deadLetter : events) {
        dataFileWriter.append(deadLetter);
      }
    }
    return outputStream.toByteArray();
  }
}
