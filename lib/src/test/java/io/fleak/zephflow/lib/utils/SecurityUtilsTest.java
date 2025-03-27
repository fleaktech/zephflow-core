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
package io.fleak.zephflow.lib.utils;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetAddress;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SecurityUtilsTest {

  @Disabled
  @Test
  @DisplayName("Test isUrlAllowed with allowed URLs")
  void testIsUrlAllowed_AllowedUrls() {
    assertTrue(SecurityUtils.isUrlAllowed("http://www.google.com"));
    assertTrue(SecurityUtils.isUrlAllowed("https://example.com"));
    assertTrue(SecurityUtils.isUrlAllowed("http://stackoverflow.com/questions"));
  }

  @Test
  @DisplayName("Test isUrlAllowed with disallowed URLs (private IPs)")
  void testIsUrlAllowed_DisallowedUrls_PrivateIPs() {
    assertFalse(SecurityUtils.isUrlAllowed("http://localhost"));
    assertFalse(SecurityUtils.isUrlAllowed("http://127.0.0.1"));
    assertFalse(SecurityUtils.isUrlAllowed("http://192.168.1.1"));
    assertFalse(SecurityUtils.isUrlAllowed("http://10.0.0.1"));
    assertFalse(SecurityUtils.isUrlAllowed("http://172.16.0.1"));
  }

  @Test
  @DisplayName("Test isUrlAllowed with invalid URLs")
  void testIsUrlAllowed_InvalidUrls() {
    assertFalse(SecurityUtils.isUrlAllowed("invalid-url"));
    assertFalse(SecurityUtils.isUrlAllowed("ftp://example.com"));
    assertFalse(SecurityUtils.isUrlAllowed("http://"));
    assertFalse(SecurityUtils.isUrlAllowed(""));
    assertFalse(SecurityUtils.isUrlAllowed(null));
  }

  @Test
  @DisplayName("Test isIpPrivate with private IPs")
  void testIsIpPrivate_PrivateIPs() throws Exception {
    assertTrue(SecurityUtils.isIpPrivate(InetAddress.getByName("127.0.0.1")));
    assertTrue(SecurityUtils.isIpPrivate(InetAddress.getByName("10.0.0.1")));
    assertTrue(SecurityUtils.isIpPrivate(InetAddress.getByName("192.168.1.1")));
    assertTrue(SecurityUtils.isIpPrivate(InetAddress.getByName("172.16.0.1")));
  }

  @Test
  @DisplayName("Test isIpPrivate with public IPs")
  void testIsIpPrivate_PublicIPs() throws Exception {
    assertFalse(SecurityUtils.isIpPrivate(InetAddress.getByName("8.8.8.8"))); // Google DNS
    assertFalse(SecurityUtils.isIpPrivate(InetAddress.getByName("1.1.1.1"))); // Cloudflare DNS
    assertFalse(SecurityUtils.isIpPrivate(InetAddress.getByName("93.184.216.34"))); // example.com
  }
}
