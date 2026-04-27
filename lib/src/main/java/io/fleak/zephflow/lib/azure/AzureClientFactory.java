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
package io.fleak.zephflow.lib.azure;

import com.azure.core.http.HttpClient;
import com.azure.core.http.okhttp.OkHttpAsyncHttpClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;

@Slf4j
public class AzureClientFactory {

  private static final HttpClient HTTP_CLIENT = buildDaemonHttpClient();

  private static HttpClient buildDaemonHttpClient() {
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            0,
            Integer.MAX_VALUE,
            60,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            r -> {
              Thread t = new Thread(r, "azure-okhttp");
              t.setDaemon(true);
              return t;
            });
    OkHttpClient okHttpClient =
        new OkHttpClient.Builder()
            .connectionPool(new ConnectionPool(5, 5, TimeUnit.MINUTES))
            .dispatcher(new Dispatcher(executor))
            .build();
    return new OkHttpAsyncHttpClientBuilder(okHttpClient).build();
  }

  public BlobServiceClient createBlobServiceClientFromConnectionString(String connectionString) {
    log.info("Creating Azure BlobServiceClient using connection string");
    return new BlobServiceClientBuilder()
        .httpClient(HTTP_CLIENT)
        .connectionString(connectionString)
        .buildClient();
  }

  public BlobServiceClient createBlobServiceClientFromAccountKey(
      String accountName, String accountKey) {
    log.info("Creating Azure BlobServiceClient for account: {}", accountName);
    StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
    return new BlobServiceClientBuilder()
        .httpClient(HTTP_CLIENT)
        .endpoint("https://" + accountName + ".blob.core.windows.net")
        .credential(credential)
        .buildClient();
  }
}
