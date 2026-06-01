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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/** Built-in {@link PostAction} factories. */
public final class PostActions {

  private PostActions() {}

  public static PostAction noOp() {
    return PostAction.NO_OP;
  }

  /** Delete via the backend. Throws if the backend does not advertise DELETE. */
  public static PostAction delete() {
    return (file, backend) -> {
      if (!backend.capabilities().contains(FsBackend.Capability.DELETE)) {
        throw new UnsupportedOperationException(
            "Backend " + backend.scheme() + " does not support DELETE");
      }
      switch (file.key().backend()) {
        case "file" -> Files.delete(Paths.get(java.net.URI.create(file.key().urn())));
        default ->
            throw new UnsupportedOperationException(
                "Delete not implemented for backend " + file.key().backend() + " in v1");
      }
    };
  }

  /** Move to a sibling prefix. Behavior depends on backend; local FS is implemented here. */
  public static PostAction moveTo(String destinationPrefix) {
    return (file, backend) -> {
      if (!backend.capabilities().contains(FsBackend.Capability.MOVE)) {
        throw new UnsupportedOperationException(
            "Backend " + backend.scheme() + " does not support MOVE");
      }
      if (!"file".equals(file.key().backend())) {
        throw new UnsupportedOperationException(
            "MoveTo not implemented for backend " + file.key().backend() + " in v1");
      }
      Path src = Paths.get(java.net.URI.create(file.key().urn()));
      Path dst =
          Paths.get(java.net.URI.create(destinationPrefix + "/" + src.getFileName().toString()));
      Files.createDirectories(dst.getParent());
      Files.move(src, dst, StandardCopyOption.ATOMIC_MOVE);
    };
  }
}
