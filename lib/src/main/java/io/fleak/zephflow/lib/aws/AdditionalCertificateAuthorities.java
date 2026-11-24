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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.stream.Stream;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

/**
 * The trust of one client that accepts what the JVM's default trust managers accept and, besides,
 * certificates issued by the given authorities. Nothing JVM-wide changes: other clients keep the
 * default trust.
 */
final class AdditionalCertificateAuthorities {

  private AdditionalCertificateAuthorities() {}

  /**
   * @param pemCertificates one or more PEM certificates of the authorities to trust besides the
   *     defaults
   * @throws IllegalArgumentException when the text holds no certificate or one that cannot be read
   */
  static TrustManager[] trustManagersWithDefaultsAnd(String pemCertificates) {
    List<X509Certificate> authorities = parse(pemCertificates);
    try {
      KeyStore trusted = KeyStore.getInstance(KeyStore.getDefaultType());
      trusted.load(null, null);
      int entry = 0;
      for (X509Certificate authority : defaultAuthorities()) {
        trusted.setCertificateEntry("default-" + entry++, authority);
      }
      for (X509Certificate authority : authorities) {
        trusted.setCertificateEntry("additional-" + entry++, authority);
      }
      TrustManagerFactory factory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      factory.init(trusted);
      return factory.getTrustManagers();
    } catch (GeneralSecurityException | IOException e) {
      throw new IllegalStateException("The client's trust could not be set up", e);
    }
  }

  private static List<X509Certificate> parse(String pemCertificates) {
    List<X509Certificate> certificates;
    try {
      certificates =
          CertificateFactory.getInstance("X.509")
              .generateCertificates(
                  new ByteArrayInputStream(pemCertificates.getBytes(StandardCharsets.US_ASCII)))
              .stream()
              .filter(X509Certificate.class::isInstance)
              .map(X509Certificate.class::cast)
              .toList();
    } catch (CertificateException e) {
      throw new IllegalArgumentException(
          "The additional CA certificates are not readable PEM certificates", e);
    }
    if (certificates.isEmpty()) {
      throw new IllegalArgumentException("The additional CA certificates hold no PEM certificate");
    }
    return certificates;
  }

  private static List<X509Certificate> defaultAuthorities() throws GeneralSecurityException {
    TrustManagerFactory factory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    factory.init((KeyStore) null);
    return Stream.of(factory.getTrustManagers())
        .filter(X509TrustManager.class::isInstance)
        .flatMap(manager -> Stream.of(((X509TrustManager) manager).getAcceptedIssuers()))
        .toList();
  }
}
