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

import io.fleak.zephflow.lib.commands.fssource.api.*;
import java.net.URI;
import java.util.Set;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public final class S3Backend implements FsBackend {

  public static final String SCHEME = "s3";

  @Override
  public String scheme() {
    return SCHEME;
  }

  @Override
  public FileLister createLister(FsBackendConfig cfg) {
    return new S3Lister(client((S3BackendConfig) cfg));
  }

  @Override
  public FileReader createReader(FsBackendConfig cfg) {
    return new S3Reader(client((S3BackendConfig) cfg));
  }

  @Override
  public Set<Capability> capabilities() {
    return Set.of(Capability.DELETE, Capability.MOVE, Capability.RANGE_READ);
  }

  public static S3Client client(S3BackendConfig cfg) {
    S3ClientBuilder b = S3Client.builder().region(Region.of(cfg.region()));
    if (cfg.accessKeyId() != null
        && !cfg.accessKeyId().isBlank()
        && cfg.secretAccessKey() != null
        && !cfg.secretAccessKey().isBlank()) {
      b.credentialsProvider(
          StaticCredentialsProvider.create(
              AwsBasicCredentials.create(cfg.accessKeyId(), cfg.secretAccessKey())));
    }
    if (cfg.s3EndpointOverride() != null && !cfg.s3EndpointOverride().isBlank()) {
      b.endpointOverride(URI.create(cfg.s3EndpointOverride()));
      b.forcePathStyle(true);
    }
    return b.build();
  }
}
