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

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.lib.commands.DefaultExecutionContext;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.io.IOException;
import lombok.EqualsAndHashCode;
import lombok.Value;
import software.amazon.awssdk.services.s3.S3Client;

@EqualsAndHashCode(callSuper = true)
@Value
public class S3FileReaderExecutionContext extends DefaultExecutionContext {
  S3FileReaderDto.Config config;
  S3Client s3Client;
  FleakDeserializer<?> deserializer;

  public S3FileReaderExecutionContext(
      FleakCounter inputMessageCounter,
      FleakCounter outputMessageCounter,
      FleakCounter errorCounter,
      S3FileReaderDto.Config config,
      S3Client s3Client,
      FleakDeserializer<?> deserializer) {
    super(inputMessageCounter, outputMessageCounter, errorCounter);
    this.config = config;
    this.s3Client = s3Client;
    this.deserializer = deserializer;
  }

  @Override
  public void close() throws IOException {
    if (s3Client != null) {
      s3Client.close();
    }
  }
}
