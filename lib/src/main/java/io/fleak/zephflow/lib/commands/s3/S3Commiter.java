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
package io.fleak.zephflow.lib.commands.s3;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.util.List;
import lombok.Getter;
import software.amazon.awssdk.services.s3.S3Client;

/** Created by bolei on 4/25/25 */
public abstract class S3Commiter<T> implements Closeable {

  @VisibleForTesting @Getter protected final S3Client s3Client;
  protected final String bucketName;

  protected S3Commiter(S3Client s3Client, String bucketName) {
    this.s3Client = s3Client;
    this.bucketName = bucketName;
  }

  /**
   * Commits the events to s3. Returns the data size in bytes
   *
   * @param events events to be commited to s3
   * @return number of bytes written to s3
   * @throws Exception any exception while commiting to s3
   */
  public abstract long commit(List<T> events) throws Exception;
}
