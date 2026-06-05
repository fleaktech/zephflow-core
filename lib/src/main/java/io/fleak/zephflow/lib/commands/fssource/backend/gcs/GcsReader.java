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
package io.fleak.zephflow.lib.commands.fssource.backend.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;

public final class GcsReader implements FileReader {

  private final Storage storage;

  public GcsReader(Storage storage) {
    this.storage = storage;
  }

  @Override
  public InputStream open(FileKey key, long offset) {
    String stripped = key.urn().substring("gs://".length());
    int slash = stripped.indexOf('/');
    String bucket = stripped.substring(0, slash);
    String objectKey = stripped.substring(slash + 1);
    ReadChannel rc = storage.reader(BlobId.of(bucket, objectKey));
    if (offset > 0) {
      try {
        rc.seek(offset);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return Channels.newInputStream(rc);
  }
}
