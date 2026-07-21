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
package io.fleak.zephflow.lib.commands.fssource;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackend;
import io.fleak.zephflow.lib.commands.fssource.backend.gcs.GcsBackend;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import io.fleak.zephflow.lib.commands.fssource.backend.s3.S3Backend;
import io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackend;
import io.fleak.zephflow.lib.commands.source.SourceCommandFactory;

public final class FsSourceCommandFactory extends SourceCommandFactory {

  static {
    FsBackendRegistry.register(new LocalFsBackend());
    FsBackendRegistry.register(new S3Backend());
    FsBackendRegistry.register(new GcsBackend());
    FsBackendRegistry.register(new AzureBackend());
    FsBackendRegistry.register(new SftpBackend());
  }

  @Override
  public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
    return new FsSourceCommand(nodeId, jobContext);
  }
}
