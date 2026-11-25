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
package io.fleak.zephflow.lib.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.junit.jupiter.api.Test;

class AdditionalCertificateAuthoritiesTest {

  @Test
  void trustsEveryDefaultAuthorityAndTheGivenOnes() throws Exception {
    X509Certificate own = TestCertificateAuthority.create("Additional Test CA").certificate();
    TrustManagerFactory defaults =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    defaults.init((KeyStore) null);
    Set<X509Certificate> expected = new HashSet<>(acceptedIssuers(defaults.getTrustManagers()));
    expected.add(own);

    assertEquals(
        expected,
        Set.copyOf(
            acceptedIssuers(
                AdditionalCertificateAuthorities.trustManagersWithDefaultsAnd(List.of(own)))));
  }

  private static List<X509Certificate> acceptedIssuers(TrustManager[] trustManagers) {
    return Stream.of(trustManagers)
        .filter(X509TrustManager.class::isInstance)
        .flatMap(manager -> Stream.of(((X509TrustManager) manager).getAcceptedIssuers()))
        .toList();
  }
}
