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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import io.fleak.zephflow.lib.aws.AwsUtils;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import software.amazon.awssdk.services.s3.S3Client;

public final class S3RegionalClientProvider implements AutoCloseable {
  private final Function<String, S3Client> clientFactory;
  private final Map<String, S3Client> clients = new LinkedHashMap<>();
  private boolean closed;

  public S3RegionalClientProvider(Function<String, S3Client> clientFactory) {
    this.clientFactory = Objects.requireNonNull(clientFactory);
  }

  public synchronized S3Client clientFor(String region) {
    if (closed) {
      throw new IllegalStateException("S3 regional clients are closed");
    }
    String regionId = AwsUtils.parseRegion(region).id();
    return clients.computeIfAbsent(regionId, clientFactory);
  }

  @Override
  public synchronized void close() {
    closed = true;
    RuntimeException failure = null;
    for (Map.Entry<String, S3Client> entry : clients.entrySet()) {
      try {
        entry.getValue().close();
      } catch (RuntimeException e) {
        RuntimeException regionFailure =
            new IllegalStateException("failed to close S3 client for region " + entry.getKey(), e);
        if (failure == null) {
          failure = regionFailure;
        } else {
          failure.addSuppressed(regionFailure);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }
}
