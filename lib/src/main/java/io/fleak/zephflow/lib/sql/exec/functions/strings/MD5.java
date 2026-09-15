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

/**
 * SQL dialect function {@code md5(input)}, matching the semantics of the same function in Postgres
 * and MySQL.
 *
 * <p>MD5 is used here as a content fingerprint for data processing — deduplication, partition keys,
 * joining against upstream systems that already store MD5 digests. It is deliberately NOT used for
 * passwords, signatures, or any other security decision, and nothing in zephflow treats its output
 * as a security boundary.
 *
 * <p>The algorithm is therefore part of this function's contract: users write {@code md5(col)} in
 * their pipelines and expect the 32-character digest that every other SQL engine produces.
 * Substituting SHA-256 would change the output of existing pipelines and break joins against
 * externally stored digests. Callers that need a cryptographic digest should use a SHA-2 function
 * instead of this one.
 */
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
      // nosemgrep - non-cryptographic content fingerprint; see the class javadoc for why the
      // algorithm is part of this SQL function's contract and cannot be substituted.
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.update(input.toString().getBytes(StandardCharsets.UTF_8));
      byte[] digest = md.digest();
      StringBuilder sb = new StringBuilder();
      for (byte b : digest) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("MD5 algorithm not found", e);
    }
  }
}
