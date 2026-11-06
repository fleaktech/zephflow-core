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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The test server's certificate is self-signed and names splunk-hec.invalid, not localhost. */
class SplunkHecSinkHttpClientTest {

  private HttpsServer server;

  @BeforeEach
  void startServer() throws Exception {
    char[] password = "changeit".toCharArray();
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    try (InputStream in = getClass().getResourceAsStream("/splunkhecsink/mismatched-host.p12")) {
      keyStore.load(in, password);
    }
    KeyManagerFactory keyManagers =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagers.init(keyStore, password);
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(keyManagers.getKeyManagers(), null, null);

    server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.setHttpsConfigurator(new HttpsConfigurator(sslContext));
    server.createContext(
        "/",
        exchange -> {
          exchange.sendResponseHeaders(200, -1);
          exchange.close();
        });
    server.start();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  private int get(HttpClient client) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(
                URI.create("https://localhost:" + server.getAddress().getPort() + "/"))
            .build();
    return client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
  }

  @Test
  void verifySslOff_acceptsACertificateForAnotherHost() throws Exception {
    assertEquals(200, get(SplunkHecSinkCommand.buildHttpClient(false)));
  }

  @Test
  void trustAllManager_acceptsAnyChainFromEveryEntryPoint() throws Exception {
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    try (InputStream in = getClass().getResourceAsStream("/splunkhecsink/mismatched-host.p12")) {
      keyStore.load(in, "changeit".toCharArray());
    }
    X509Certificate[] chain = {(X509Certificate) keyStore.getCertificate("hec")};
    SplunkHecSinkCommand.TrustAllManager manager = new SplunkHecSinkCommand.TrustAllManager();
    SSLEngine engine = SSLContext.getDefault().createSSLEngine();

    assertAll(
        () -> manager.checkServerTrusted(chain, "RSA"),
        () -> manager.checkClientTrusted(chain, "RSA"),
        () -> manager.checkServerTrusted(chain, "RSA", engine),
        () -> manager.checkClientTrusted(chain, "RSA", engine),
        () -> manager.checkServerTrusted(chain, "RSA", (Socket) null),
        () -> manager.checkClientTrusted(chain, "RSA", (Socket) null),
        () -> assertArrayEquals(new X509Certificate[0], manager.getAcceptedIssuers()));
  }

  @Test
  void verifySslOn_rejectsAnUntrustedCertificate() {
    assertThrows(
        SSLHandshakeException.class, () -> get(SplunkHecSinkCommand.buildHttpClient(true)));
  }
}
