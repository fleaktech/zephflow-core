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

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/** A certificate authority of a test, which issues the certificate of a server. */
public final class TestCertificateAuthority {

  private final KeyPair keyPair;
  private final X509Certificate certificate;

  private TestCertificateAuthority(KeyPair keyPair, X509Certificate certificate) {
    this.keyPair = keyPair;
    this.certificate = certificate;
  }

  public static TestCertificateAuthority create(String name) {
    KeyPair keyPair = newKeyPair();
    X500Name subject = new X500Name("CN=" + name);
    return new TestCertificateAuthority(
        keyPair, sign(subject, subject, keyPair, keyPair, new BasicConstraints(true), null));
  }

  public X509Certificate certificate() {
    return certificate;
  }

  public String certificatePem() {
    try {
      StringWriter writer = new StringWriter();
      try (JcaPEMWriter pemWriter = new JcaPEMWriter(writer)) {
        pemWriter.writeObject(certificate);
      }
      return writer.toString();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** The TLS context of a server on 127.0.0.1 whose certificate this authority issued. */
  public SSLContext serverContext() {
    return serverContext("CN=127.0.0.1", new GeneralName(GeneralName.iPAddress, "127.0.0.1"));
  }

  /**
   * The TLS context of a server whose certificate this authority issued for {@code dnsName} only,
   * which a client that reaches it at another name must refuse.
   */
  public SSLContext serverContextForDnsName(String dnsName) {
    return serverContext("CN=" + dnsName, new GeneralName(GeneralName.dNSName, dnsName));
  }

  private SSLContext serverContext(String subject, GeneralName subjectAlternativeName) {
    try {
      KeyPair serverKeyPair = newKeyPair();
      X509Certificate serverCertificate =
          sign(
              new X500Name(subject),
              X500Name.getInstance(certificate.getSubjectX500Principal().getEncoded()),
              serverKeyPair,
              keyPair,
              new BasicConstraints(false),
              subjectAlternativeName);
      char[] keyPassword = new char[0];
      KeyStore keyStore = KeyStore.getInstance("PKCS12");
      keyStore.load(null, null);
      keyStore.setKeyEntry(
          "server",
          serverKeyPair.getPrivate(),
          keyPassword,
          new Certificate[] {serverCertificate, certificate});
      KeyManagerFactory keyManagers =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyManagers.init(keyStore, keyPassword);
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(keyManagers.getKeyManagers(), null, null);
      return context;
    } catch (GeneralSecurityException | IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static X509Certificate sign(
      X500Name subject,
      X500Name issuer,
      KeyPair subjectKeyPair,
      KeyPair issuerKeyPair,
      BasicConstraints constraints,
      GeneralName subjectAlternativeName) {
    try {
      Instant now = Instant.now();
      JcaX509v3CertificateBuilder builder =
          new JcaX509v3CertificateBuilder(
              issuer,
              BigInteger.valueOf(new SecureRandom().nextLong(Long.MAX_VALUE)),
              Date.from(now.minus(Duration.ofDays(1))),
              Date.from(now.plus(Duration.ofDays(1))),
              subject,
              subjectKeyPair.getPublic());
      builder.addExtension(Extension.basicConstraints, true, constraints);
      if (subjectAlternativeName != null) {
        builder.addExtension(
            Extension.subjectAlternativeName, false, new GeneralNames(subjectAlternativeName));
      }
      return new JcaX509CertificateConverter()
          .getCertificate(
              builder.build(
                  new JcaContentSignerBuilder("SHA256withECDSA")
                      .build(issuerKeyPair.getPrivate())));
    } catch (GeneralSecurityException | IOException | OperatorCreationException e) {
      throw new IllegalStateException(e);
    }
  }

  private static KeyPair newKeyPair() {
    try {
      KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
      generator.initialize(new ECGenParameterSpec("secp256r1"));
      return generator.generateKeyPair();
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }
}
