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

import io.fleak.zephflow.api.structure.*;
import io.fleak.zephflow.lib.utils.JsonUtils;
import org.junit.jupiter.api.Test;

class CidrMatchFunctionTest extends FeelFunctionTestBase {

  private static final FleakData DUMMY = new StringPrimitiveFleakData("x");

  @Test
  public void testIpv4() {
    testFunctionExecution(DUMMY, "cidr_match(\"10.1.2.3\", \"10.0.0.0/8\")", true);
    testFunctionExecution(DUMMY, "cidr_match(\"11.1.2.3\", \"10.0.0.0/8\")", false);
    testFunctionExecution(DUMMY, "cidr_match(\"192.168.1.5\", \"192.168.1.0/24\")", true);
    testFunctionExecution(DUMMY, "cidr_match(\"192.168.2.5\", \"192.168.1.0/24\")", false);
    testFunctionExecution(DUMMY, "cidr_match(\"10.0.0.1\", \"10.0.0.1/32\")", true);
  }

  @Test
  public void testIpv6() {
    testFunctionExecution(DUMMY, "cidr_match(\"2001:db8::1\", \"2001:db8::/32\")", true);
    testFunctionExecution(DUMMY, "cidr_match(\"2001:dead::1\", \"2001:db8::/32\")", false);
  }

  @Test
  public void testMalformedOrMixedFamily() {
    testFunctionExecution(DUMMY, "cidr_match(\"10.1.2.3\", \"2001:db8::/32\")", false);
    testFunctionExecution(DUMMY, "cidr_match(\"not-an-ip\", \"10.0.0.0/8\")", false);
    testFunctionExecution(DUMMY, "cidr_match(\"10.1.2.3\", \"10.0.0.0\")", false);
  }

  @Test
  public void testArrayMatchesAnyElement() {
    FleakData hit = JsonUtils.loadFleakDataFromJsonString("{\"ips\":[\"8.8.8.8\",\"10.1.2.3\"]}");
    testFunctionExecution(hit, "cidr_match($.ips, \"10.0.0.0/8\")", true);
    FleakData miss = JsonUtils.loadFleakDataFromJsonString("{\"ips\":[\"8.8.8.8\",\"1.2.3.4\"]}");
    testFunctionExecution(miss, "cidr_match($.ips, \"10.0.0.0/8\")", false);
  }

  @Test
  public void testPrefixBoundaries() {
    // /0 matches any address of the same family
    testFunctionExecution(DUMMY, "cidr_match(\"203.0.113.9\", \"0.0.0.0/0\")", true);
    // an out-of-range prefix is rejected (not treated as a wider mask)
    testFunctionExecution(DUMMY, "cidr_match(\"10.1.2.3\", \"10.0.0.0/33\")", false);
  }

  @Test
  public void testMissingOrNonStringValueIsFalse() {
    testFunctionExecution(
        JsonUtils.loadFleakDataFromJsonString("{\"g\":1}"),
        "cidr_match($.ip, \"10.0.0.0/8\")",
        false);
    testFunctionExecution(
        JsonUtils.loadFleakDataFromJsonString("{\"ip\":{\"n\":1}}"),
        "cidr_match($.ip, \"10.0.0.0/8\")",
        false);
  }
}
