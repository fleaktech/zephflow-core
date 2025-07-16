package io.fleak.zephflow.lib.commands.clickhouse;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ClickHouseSinkPartsFactoryTest {

  @Test
  public void testCreateFlusher() {
    MetricClientProvider metricClientProvider = mock();
    JobContext jobContext = mock();
    ClickHouseSinkDto.Config config =
            ClickHouseSinkDto.Config.builder()
                    .username("admin")
                    .password("password")
                    .endpoint("http://localhost:8123")
                    .database("analytics")
                    .table("events")
                    .credentialId(null)
                    .build();

    ClickHouseSinkPartsFactory factory =
            new ClickHouseSinkPartsFactory(metricClientProvider, jobContext, config);

    SimpleSinkCommand.Flusher<?> flusher = factory.createFlusher();
    assertNotNull(flusher, "Flusher should not be null");
    assertInstanceOf(ClickHouseWriter.class, flusher, "Flusher should be ClickHouseWriter");
  }
}
