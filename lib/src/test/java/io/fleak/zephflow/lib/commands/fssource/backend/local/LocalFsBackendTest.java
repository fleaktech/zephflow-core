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
package io.fleak.zephflow.lib.commands.fssource.backend.local;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.fssource.api.FileLister;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import io.fleak.zephflow.lib.commands.fssource.api.ListRequest;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalFsBackendTest {

  @Test
  void listsMatchingFilesAndIgnoresOthers(@TempDir Path tmp) throws IOException {
    Files.writeString(tmp.resolve("invoice_1.json"), "a");
    Files.writeString(tmp.resolve("invoice_2.json"), "bb");
    Files.writeString(tmp.resolve("other.txt"), "c");

    LocalFsBackend backend = new LocalFsBackend();
    FileLister lister = backend.createLister(new LocalFsBackendConfig(tmp.toString()));

    List<FileEntry> out =
        lister
            .list(new ListRequest(tmp.toString(), Pattern.compile("invoice_\\d+\\.json")))
            .toList();

    assertEquals(2, out.size());
    assertTrue(out.stream().allMatch(e -> e.key().backend().equals("file")));
    assertTrue(out.stream().anyMatch(e -> e.displayPath().endsWith("invoice_1.json")));
  }

  @Test
  void readerStreamsFile(@TempDir Path tmp) throws Exception {
    Path p = tmp.resolve("data.bin");
    Files.writeString(p, "hello-world");

    LocalFsBackend backend = new LocalFsBackend();
    FileReader reader = backend.createReader(new LocalFsBackendConfig(tmp.toString()));

    try (InputStream in =
        reader.open(
            new io.fleak.zephflow.lib.commands.fssource.api.FileKey("file", p.toUri().toString()),
            0)) {
      assertEquals("hello-world", new String(in.readAllBytes()));
    }
  }

  @Test
  void statReturnsCurrentSize(@TempDir Path tmp) throws Exception {
    Path p = tmp.resolve("x");
    Files.writeString(p, "abc");
    LocalFsBackend backend = new LocalFsBackend();
    FileLister lister = backend.createLister(new LocalFsBackendConfig(tmp.toString()));
    FileEntry e =
        lister.stat(
            new io.fleak.zephflow.lib.commands.fssource.api.FileKey("file", p.toUri().toString()));
    assertEquals(3, e.size());
  }
}
