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
package io.fleak.zephflow.lib.commands.fssource.api;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class FsBackendRegistryTest {

  private static class FakeBackend implements FsBackend {
    @Override
    public String scheme() {
      return "fake";
    }

    @Override
    public FileLister createLister(FsBackendConfig cfg) {
      return new FileLister() {
        @Override
        public Stream<FileEntry> list(ListRequest r) {
          return Stream.empty();
        }

        @Override
        public FileEntry stat(FileKey k) {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public FileReader createReader(FsBackendConfig cfg) {
      return (k, offset) -> {
        throw new UnsupportedOperationException();
      };
    }

    @Override
    public Set<Capability> capabilities() {
      return Set.of();
    }
  }

  @AfterEach
  void cleanup() {
    FsBackendRegistry.unregister("fake");
  }

  @Test
  void registerAndGet() {
    FakeBackend b = new FakeBackend();
    FsBackendRegistry.register(b);
    assertSame(b, FsBackendRegistry.get("fake"));
  }

  @Test
  void getUnknown_throws() {
    assertThrows(IllegalArgumentException.class, () -> FsBackendRegistry.get("nope"));
  }

  @Test
  void duplicateRegister_throws() {
    FsBackendRegistry.register(new FakeBackend());
    assertThrows(IllegalStateException.class, () -> FsBackendRegistry.register(new FakeBackend()));
  }
}
