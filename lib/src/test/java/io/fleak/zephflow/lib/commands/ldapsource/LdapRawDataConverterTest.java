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
package io.fleak.zephflow.lib.commands.ldapsource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LdapRawDataConverterTest {

  private LdapRawDataConverter converter;
  private SourceExecutionContext<?> mockContext;
  private FleakCounter mockInputEventCounter;
  private FleakCounter mockDataSizeCounter;
  private FleakCounter mockDeserializeFailureCounter;

  @BeforeEach
  void setUp() {
    converter = new LdapRawDataConverter();
    mockContext = mock(SourceExecutionContext.class);
    mockInputEventCounter = mock(FleakCounter.class);
    mockDataSizeCounter = mock(FleakCounter.class);
    mockDeserializeFailureCounter = mock(FleakCounter.class);

    when(mockContext.inputEventCounter()).thenReturn(mockInputEventCounter);
    when(mockContext.dataSizeCounter()).thenReturn(mockDataSizeCounter);
    when(mockContext.deserializeFailureCounter()).thenReturn(mockDeserializeFailureCounter);
  }

  @Test
  void testSingleValuedAttributesBecomeStrings() {
    Map<String, List<String>> attributes = new LinkedHashMap<>();
    attributes.put("cn", List.of("John Doe"));
    attributes.put("mail", List.of("jdoe@example.com"));

    LdapEntry entry = new LdapEntry("uid=jdoe,ou=users,dc=example,dc=com", attributes);

    var metricProvider = new MetricClientProvider.NoopMetricClientProvider();
    var context =
        new SourceExecutionContext<>(
            null,
            null,
            null,
            metricProvider.counter("data_size", Map.of()),
            metricProvider.counter("input_event", Map.of()),
            metricProvider.counter("deser_error", Map.of()),
            null);

    ConvertedResult<LdapEntry> result = converter.convert(entry, context);

    assertNotNull(result);
    assertNull(result.error());
    assertNotNull(result.transformedData());
    assertEquals(1, result.transformedData().size());

    RecordFleakData record = result.transformedData().get(0);
    assertEquals("uid=jdoe,ou=users,dc=example,dc=com", record.getPayload().get("dn").unwrap());
    assertEquals("John Doe", record.getPayload().get("cn").unwrap());
    assertEquals("jdoe@example.com", record.getPayload().get("mail").unwrap());
  }

  @Test
  void testMultiValuedAttributesBecomeLists() {
    Map<String, List<String>> attributes = new LinkedHashMap<>();
    attributes.put("cn", List.of("John Doe"));
    attributes.put("mail", List.of("jdoe@example.com", "john.doe@corp.com"));

    LdapEntry entry = new LdapEntry("uid=jdoe,ou=users,dc=example,dc=com", attributes);

    var metricProvider = new MetricClientProvider.NoopMetricClientProvider();
    var context =
        new SourceExecutionContext<>(
            null,
            null,
            null,
            metricProvider.counter("data_size", Map.of()),
            metricProvider.counter("input_event", Map.of()),
            metricProvider.counter("deser_error", Map.of()),
            null);

    ConvertedResult<LdapEntry> result = converter.convert(entry, context);

    assertNotNull(result);
    assertNull(result.error());
    RecordFleakData record = result.transformedData().get(0);

    assertEquals("John Doe", record.getPayload().get("cn").unwrap());

    @SuppressWarnings("unchecked")
    List<Object> mailValues = (List<Object>) record.getPayload().get("mail").unwrap();
    assertEquals(2, mailValues.size());
    assertEquals("jdoe@example.com", mailValues.get(0));
    assertEquals("john.doe@corp.com", mailValues.get(1));
  }

  @Test
  void testMetricsAreUpdated() {
    Map<String, List<String>> attributes = new LinkedHashMap<>();
    attributes.put("cn", List.of("Test User"));

    LdapEntry entry = new LdapEntry("uid=test,dc=example,dc=com", attributes);

    ConvertedResult<LdapEntry> result = converter.convert(entry, mockContext);

    assertNull(result.error());
    assertEquals(1, result.transformedData().size());

    verify(mockDataSizeCounter).increase(anyLong(), any());
    verify(mockInputEventCounter).increase(eq(1L), any());
    verify(mockDeserializeFailureCounter, never()).increase(any());
  }
}
