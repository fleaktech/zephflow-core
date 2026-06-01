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
package io.fleak.zephflow.lib.commands.fssource.backend.s3;

import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import java.io.InputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public final class S3Reader implements FileReader {

  private final S3Client client;

  public S3Reader(S3Client client) {
    this.client = client;
  }

  @Override
  public InputStream open(FileKey key, long offset) {
    String stripped = key.urn().substring("s3://".length());
    int slash = stripped.indexOf('/');
    String bucket = stripped.substring(0, slash);
    String objectKey = stripped.substring(slash + 1);
    GetObjectRequest.Builder b = GetObjectRequest.builder().bucket(bucket).key(objectKey);
    if (offset > 0) b.range("bytes=" + offset + "-");
    return client.getObject(b.build());
  }

  @Override
  public void close() {
    client.close();
  }
}
