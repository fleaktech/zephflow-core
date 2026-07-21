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
package io.fleak.zephflow.lib.commands.fssource.backend.sftp;

import io.fleak.zephflow.lib.commands.fssource.api.FileLister;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackend;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendConfig;
import java.util.Set;

public final class SftpBackend implements FsBackend {

  @Override
  public String scheme() {
    return SftpBackendConfig.SCHEME;
  }

  @Override
  public FileLister createLister(FsBackendConfig cfg) {
    return new SftpLister((SftpBackendConfig) cfg);
  }

  @Override
  public FileReader createReader(FsBackendConfig cfg) {
    return new SftpReader((SftpBackendConfig) cfg);
  }

  @Override
  public Set<Capability> capabilities() {
    return Set.of(Capability.DELETE, Capability.MOVE, Capability.RANGE_READ);
  }
}
