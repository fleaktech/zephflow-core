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

import java.net.InetAddress;
import java.net.URI;

/** Created by bolei on 9/19/24 */
public interface SecurityUtils {
  static boolean isUrlAllowed(String url) {
    try {
      URI uri = new URI(url);

      String scheme = uri.getScheme();
      if (scheme == null
          || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
        return false; // Disallow non-http/https schemes
      }
      String host = uri.getHost();
      if (host == null) {
        return false; // Invalid host
      }
      // Resolve hostname to IP addresses
      InetAddress inetAddress = InetAddress.getByName(host);
      return !inetAddress.isAnyLocalAddress()
          && !inetAddress.isLoopbackAddress()
          && !inetAddress.isSiteLocalAddress()
          && !isIpPrivate(inetAddress);
    } catch (Exception e) {
      return false; // Block invalid URLs
    }
  }

  static boolean isIpPrivate(InetAddress inetAddress) {
    byte[] address = inetAddress.getAddress();
    return inetAddress.isSiteLocalAddress()
        || inetAddress.isLoopbackAddress()
        || inetAddress.isAnyLocalAddress()
        || (address[0] & 0xFF) == 10
        || ((address[0] & 0xFF) == 172 && (address[1] & 0xF0) == 16)
        || ((address[0] & 0xFF) == 192 && (address[1] & 0xFF) == 168);
  }
}
