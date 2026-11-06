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

import io.fleak.zephflow.lib.commands.sink.DestinationFailureClassifier;
import io.fleak.zephflow.lib.commands.sink.DestinationFailureException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Every upload failure (SDK client errors, any S3 status such as NoSuchBucket, 403 or 5xx, and
 * anything unrecognised) is a destination problem. Only a failure writing the local temp file is
 * not: retrying it against the destination cannot help.
 */
public class S3DestinationFailureClassifier implements DestinationFailureClassifier {
  @Override
  public boolean isDestinationFailure(Throwable t) {
    return !(unwrap(t) instanceof BatchS3Flusher.TempFileWriteException);
  }

  private static Throwable unwrap(Throwable t) {
    Throwable current = t;
    while ((current instanceof CompletionException
            || current instanceof ExecutionException
            || current instanceof DestinationFailureException)
        && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }
}
