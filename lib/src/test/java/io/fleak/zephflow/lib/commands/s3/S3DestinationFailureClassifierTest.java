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

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fleak.zephflow.lib.commands.sink.DestinationFailureException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;

class S3DestinationFailureClassifierTest {

  @Test
  void classifiesUploadFailuresAsDestinationAndTempFileFailuresAsNot() {
    Map<String, Throwable> failures = new LinkedHashMap<>();
    failures.put(
        "missing bucket",
        new CompletionException(NoSuchBucketException.builder().message("no bucket").build()));
    failures.put("403", S3Exception.builder().statusCode(403).message("denied").build());
    failures.put("503", S3Exception.builder().statusCode(503).message("slow down").build());
    failures.put("client", SdkClientException.create("unable to connect"));
    failures.put("unclassified", new RuntimeException("boom"));
    failures.put(
        "wrapped",
        new DestinationFailureException(
            new CompletionException(SdkClientException.create("down")), List.of()));
    failures.put(
        "temp file",
        new BatchS3Flusher.TempFileWriteException(new IOException("No space left on device")));

    S3DestinationFailureClassifier classifier = new S3DestinationFailureClassifier();
    Map<String, Boolean> actual = new LinkedHashMap<>();
    failures.forEach((name, failure) -> actual.put(name, classifier.isDestinationFailure(failure)));

    Map<String, Boolean> expected = new LinkedHashMap<>();
    expected.put("missing bucket", true);
    expected.put("403", true);
    expected.put("503", true);
    expected.put("client", true);
    expected.put("unclassified", true);
    expected.put("wrapped", true);
    expected.put("temp file", false);
    assertEquals(expected, actual);
  }
}
