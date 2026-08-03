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
package io.fleak.zephflow.lib.commands.eval;

import com.google.common.base.Preconditions;
import com.google.common.net.InetAddresses;
import io.fleak.zephflow.api.structure.*;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.compiled.EvalContext;
import java.net.InetAddress;
import java.util.List;

/*
cidrMatchFunction:
Tests whether an IP address falls inside a CIDR network range (IPv4 or IPv6).

Syntax:
cidr_match($.path.to.ip.field, "10.0.0.0/8")

Parameters:
- First argument: the IP address to test (string)
- Second argument: the CIDR range (string, "address/prefixLength")

Returns: true if the IP is within the range, false otherwise. Returns false when either
argument is not a parseable IP / CIDR, or the IP and range are different families (v4 vs v6).
*/
class CidrMatchFunction implements FeelFunction {

  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("cidr_match", 2, "ip address and cidr range");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    FleakData ipFd = evaluatedArgs.getFirst();
    FleakData cidrFd = evaluatedArgs.get(1);
    Preconditions.checkArgument(
        cidrFd instanceof StringPrimitiveFleakData,
        "cidr_match: cidr must be a string: %s",
        cidrFd);

    return new BooleanPrimitiveFleakData(matchesAny(ipFd, cidrFd.getStringValue()));
  }

  // Accepts a scalar or an array (matching any element), so multi-valued IP fields work.
  private static boolean matchesAny(FleakData value, String cidr) {
    if (value == null) {
      return false;
    }
    if (value instanceof ArrayFleakData array) {
      for (FleakData element : array.getArrayPayload()) {
        if (matchesAny(element, cidr)) {
          return true;
        }
      }
      return false;
    }
    if (value instanceof RecordFleakData) {
      return false;
    }
    Object raw = value.unwrap();
    return raw != null && matches(String.valueOf(raw), cidr);
  }

  private static boolean matches(String ipStr, String cidr) {
    int slash = cidr.indexOf('/');
    if (slash < 0) {
      return false;
    }
    try {
      InetAddress ip = InetAddresses.forString(ipStr.trim());
      InetAddress network = InetAddresses.forString(cidr.substring(0, slash).trim());
      int prefix = Integer.parseInt(cidr.substring(slash + 1).trim());

      byte[] ipBytes = ip.getAddress();
      byte[] networkBytes = network.getAddress();
      if (ipBytes.length != networkBytes.length) {
        return false;
      }
      if (prefix < 0 || prefix > ipBytes.length * 8) {
        return false;
      }

      int fullBytes = prefix / 8;
      for (int i = 0; i < fullBytes; i++) {
        if (ipBytes[i] != networkBytes[i]) {
          return false;
        }
      }
      int remainingBits = prefix % 8;
      if (remainingBits > 0) {
        int mask = (0xFF << (8 - remainingBits)) & 0xFF;
        return (ipBytes[fullBytes] & mask) == (networkBytes[fullBytes] & mask);
      }
      return true;
    } catch (RuntimeException e) {
      return false;
    }
  }
}
