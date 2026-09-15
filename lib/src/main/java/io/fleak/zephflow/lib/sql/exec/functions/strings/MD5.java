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
package io.fleak.zephflow.lib.sql.exec.functions.strings;

import io.fleak.zephflow.lib.sql.exec.functions.BaseFunction;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class MD5 extends BaseFunction {

  public static final String NAME = "md5";

  public MD5(TypeSystem typeSystem) {
    super(typeSystem, NAME);
  }

  @Override
  public Object apply(List<Object> args) {
    assertArgs(args, 1, "(input)");

    var input = args.getFirst();
    if (input == null) return null;

    try {
      // nosemgrep: java.lang.security.audit.crypto.use-of-md5.use-of-md5
      MessageDigest messageDigest = MessageDigest.getInstance("MD5");
      messageDigest.update(input.toString().getBytes(StandardCharsets.UTF_8));
      byte[] digest = messageDigest.digest();
      StringBuilder hexDigest = new StringBuilder();
      for (byte digestByte : digest) {
        hexDigest.append(String.format("%02x", digestByte));
      }
      return hexDigest.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("MD5 algorithm not found", e);
    }
  }
}
