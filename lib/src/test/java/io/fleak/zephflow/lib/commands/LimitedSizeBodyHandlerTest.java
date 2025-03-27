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

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Created by bolei on 9/19/24 */
class LimitedSizeBodyHandlerTest {
  private LimitedSizeBodyHandler.LimitedSizeBodySubscriber subscriber;
  private final int maxSize = 10; // Set a small limit for testing

  @BeforeEach
  void setUp() {
    subscriber = new LimitedSizeBodyHandler.LimitedSizeBodySubscriber(maxSize);
  }

  @Test
  void testOnNextWithinLimit() {
    // Create ByteBuffer chunks within the size limit
    ByteBuffer chunk1 = StandardCharsets.UTF_8.encode("Hello");
    ByteBuffer chunk2 = StandardCharsets.UTF_8.encode("World");

    // Call onNext with these chunks
    subscriber.onNext(List.of(chunk1, chunk2));

    // Complete the subscriber to get the accumulated result
    subscriber.onComplete();

    // Assert that the accumulated data is correct
    assertEquals("HelloWorld", subscriber.getBody().toCompletableFuture().join());
  }

  @Test
  void testOnNextExceedingLimit() {
    // Create ByteBuffer chunks where total size exceeds maxSize
    ByteBuffer chunk1 = StandardCharsets.UTF_8.encode("Hello");
    ByteBuffer chunk2 =
        StandardCharsets.UTF_8.encode("World!!!"); // This chunk pushes it over the limit

    // Call onNext with these chunks and check if it throws an exception
    subscriber.onNext(List.of(chunk1, chunk2));

    // Attempt to complete the subscriber to trigger the exception handling
    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () -> subscriber.getBody().toCompletableFuture().join(),
            "Expected exception due to size limit exceeded");

    // Check the exception message
    assertInstanceOf(IOException.class, exception.getCause());
    assertEquals(
        "Response body exceeds max size of " + maxSize + " bytes.",
        exception.getCause().getMessage());
  }
}
