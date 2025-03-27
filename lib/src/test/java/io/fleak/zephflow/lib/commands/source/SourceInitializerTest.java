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
package io.fleak.zephflow.lib.commands.source;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import org.junit.Before;
import org.junit.Test;

public class SourceInitializerTest {
  private static final String NODE_ID = "test-node";

  private SourceCommandPartsFactory mockPartsFactory;
  private Fetcher mockFetcher;
  private DlqWriter mockDlqWriter;
  private JobContext mockJobContext;
  private CommandConfig mockCommandConfig;
  private JobContext.DlqConfig mockDlqConfig;
  private SourceInitializer sourceInitializer;

  @Before
  public void setUp() {
    mockPartsFactory = mock(SourceCommandPartsFactory.class);
    mockFetcher = mock(Fetcher.class);
    mockDlqWriter = mock(DlqWriter.class);
    mockJobContext = mock(JobContext.class);
    mockCommandConfig = mock(CommandConfig.class);
    mockDlqConfig = mock(JobContext.DlqConfig.class);

    sourceInitializer = new SourceInitializer(NODE_ID, mockPartsFactory);
  }

  @Test
  public void testBaseClassInitialization() {
    assertEquals(NODE_ID, TestUtils.getFieldValue(sourceInitializer, "nodeId"));
    assertEquals(
        mockPartsFactory, TestUtils.getFieldValue(sourceInitializer, "commandPartsFactory"));
  }

  @Test
  public void testInitialize_WithDlq() {
    // Arrange
    String commandName = "test-command";
    when(mockJobContext.getDlqConfig()).thenReturn(mockDlqConfig);
    when(mockPartsFactory.createFetcher(mockCommandConfig)).thenReturn(mockFetcher);
    when(mockPartsFactory.createDlqWriter(mockDlqConfig)).thenReturn(mockDlqWriter);

    // Act
    SourceInitializedConfig result =
        sourceInitializer.initialize(commandName, mockJobContext, mockCommandConfig);

    // Assert
    assertNotNull("Result should not be null", result);

    assertEquals("Fetcher should match", mockFetcher, result.fetcher());
    assertEquals("DlqWriter should match", mockDlqWriter, result.dlqWriter());

    verify(mockDlqWriter).open();
    verify(mockPartsFactory).createFetcher(mockCommandConfig);
    verify(mockPartsFactory).createDlqWriter(mockDlqConfig);
  }

  @Test
  public void testInitialize_WithoutDlq() {
    // Arrange
    String commandName = "test-command";
    when(mockJobContext.getDlqConfig()).thenReturn(null);
    when(mockPartsFactory.createFetcher(mockCommandConfig)).thenReturn(mockFetcher);

    // Act
    SourceInitializedConfig result =
        sourceInitializer.initialize(commandName, mockJobContext, mockCommandConfig);

    // Assert
    assertNotNull("Result should not be null", result);

    assertEquals("Fetcher should match", mockFetcher, result.fetcher());
    assertNull("DlqWriter should be null", result.dlqWriter());

    verify(mockPartsFactory).createFetcher(mockCommandConfig);
    verify(mockPartsFactory, never()).createDlqWriter(any());
  }

  @Test
  public void testInitialize_WithNullJobContext() {
    // Arrange
    String commandName = "test-command";
    when(mockPartsFactory.createFetcher(mockCommandConfig)).thenReturn(mockFetcher);

    // Act
    SourceInitializedConfig result =
        sourceInitializer.initialize(commandName, null, mockCommandConfig);

    // Assert
    assertNotNull("Result should not be null", result);

    assertEquals("Fetcher should match", mockFetcher, result.fetcher());
    assertNull("DlqWriter should be null", result.dlqWriter());

    verify(mockPartsFactory).createFetcher(mockCommandConfig);
    verify(mockPartsFactory, never()).createDlqWriter(any());
  }
}

// Utility class to access private fields for testing
class TestUtils {
  public static Object getFieldValue(Object object, String fieldName) {
    try {
      java.lang.reflect.Field field = object.getClass().getSuperclass().getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(object);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get field value", e);
    }
  }
}
