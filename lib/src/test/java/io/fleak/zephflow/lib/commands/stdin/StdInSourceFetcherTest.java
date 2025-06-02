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
package io.fleak.zephflow.lib.commands.stdin;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Created by bolei on 12/20/24 */
class StdInSourceFetcherTest {
  private final InputStream originalSystemIn = System.in;
  private FleakDeserializer<?> mockDeserializer;
  private StdInSourceFetcher fetcher;

  @BeforeEach
  void setUp() {
    mockDeserializer = mock(FleakDeserializer.class);
    fetcher = new StdInSourceFetcher();
  }

  @AfterEach
  void tearDown() {
    System.setIn(originalSystemIn);
  }

  @Test
  void testFetchWithValidInput() throws Exception {
    // Prepare test data
    String testInput = "{\"key\":\"value\"}\n\n";
    ByteArrayInputStream testIn = new ByteArrayInputStream(testInput.getBytes());
    System.setIn(testIn);

    // Mock deserializer behavior
    RecordFleakData mockRecord = mock(RecordFleakData.class);
    when(mockDeserializer.deserialize(any(SerializedEvent.class))).thenReturn(List.of(mockRecord));

    // Execute fetch
    var result = fetcher.fetch();

    // Verify results
    assertNotNull(result);
    assertEquals(1, result.size());

    var firstResult = result.get(0);
    assertNotNull(firstResult);

    // Verify the raw data in serialized event
    byte[] expectedRawData = "{\"key\":\"value\"}".getBytes();
    assertArrayEquals(expectedRawData, firstResult.value());
  }

  @Test
  void testFetchWithEmptyInput() throws Exception {
    // Prepare empty input
    String testInput = "\n";
    ByteArrayInputStream testIn = new ByteArrayInputStream(testInput.getBytes());
    System.setIn(testIn);

    // Mock deserializer behavior
    when(mockDeserializer.deserialize(any(SerializedEvent.class))).thenReturn(List.of());

    // Execute fetch
    var result = fetcher.fetch();

    // Verify results
    assertNotNull(result);
    assertEquals(1, result.size());
  }

  @Test
  void testFetchWithIOException() {
    // Prepare a problematic InputStream that throws IOException
    InputStream troubleInput =
        new InputStream() {
          @Override
          public int read() throws IOException {
            throw new IOException("Test IOException");
          }
        };
    System.setIn(troubleInput);

    // Execute fetch
    var result = fetcher.fetch();

    // Verify results
    assertNull(result);
  }

  @Test
  void testClose() {
    // Verify that close() doesn't throw any exceptions
    assertDoesNotThrow(() -> fetcher.close());
  }
}
