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
package io.fleak.zephflow.lib.commands;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/** Created by bolei on 9/19/24 */
public class LimitedSizeBodyHandler implements HttpResponse.BodyHandler<String> {

  private final int maxSize;

  public LimitedSizeBodyHandler(int maxSize) {
    this.maxSize = maxSize;
  }

  @Override
  public HttpResponse.BodySubscriber<String> apply(HttpResponse.ResponseInfo responseInfo) {
    return new LimitedSizeBodySubscriber(maxSize);
  }

  static class LimitedSizeBodySubscriber implements HttpResponse.BodySubscriber<String> {

    private final int maxSize;
    private final CompletableFuture<String> result = new CompletableFuture<>();
    private final StringBuilder data = new StringBuilder();
    private int totalBytesReceived = 0;

    LimitedSizeBodySubscriber(int maxSize) {
      this.maxSize = maxSize;
    }

    @Override
    public CompletionStage<String> getBody() {
      return result;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      subscription.request(Long.MAX_VALUE); // Request all data
    }

    @Override
    public void onNext(List<ByteBuffer> items) {
      for (ByteBuffer item : items) {
        int chunkSize = item.remaining();
        totalBytesReceived += chunkSize;

        if (totalBytesReceived > maxSize) {
          result.completeExceptionally(
              new IOException("Response body exceeds max size of " + maxSize + " bytes."));
          return;
        }

        data.append(StandardCharsets.UTF_8.decode(item));
      }
    }

    @Override
    public void onError(Throwable throwable) {
      result.completeExceptionally(throwable);
    }

    @Override
    public void onComplete() {
      result.complete(data.toString());
    }
  }
}
