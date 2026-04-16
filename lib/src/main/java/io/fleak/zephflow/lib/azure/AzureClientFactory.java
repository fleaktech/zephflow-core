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

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import java.io.Serializable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AzureClientFactory implements Serializable {

  public BlobServiceClient createBlobServiceClientFromConnectionString(String connectionString) {
    log.info("Creating Azure BlobServiceClient using connection string");
    return new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
  }

  public BlobServiceClient createBlobServiceClientFromAccountKey(
      String accountName, String accountKey) {
    log.info("Creating Azure BlobServiceClient for account: {}", accountName);
    StorageSharedKeyCredential credential =
        new StorageSharedKeyCredential(accountName, accountKey);
    return new BlobServiceClientBuilder()
        .endpoint("https://" + accountName + ".blob.core.windows.net")
        .credential(credential)
        .buildClient();
  }
}
