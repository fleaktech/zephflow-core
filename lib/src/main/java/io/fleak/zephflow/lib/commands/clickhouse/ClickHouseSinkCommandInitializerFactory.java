package io.fleak.zephflow.lib.commands.clickhouse;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.CommandPartsFactory;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.sink.SinkCommandInitializerFactory;
import java.util.Map;

public class ClickHouseSinkCommandInitializerFactory
    extends SinkCommandInitializerFactory<Map<String, Object>> {

  @Override
  protected CommandPartsFactory createCommandPartsFactory(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    return new ClickHouseSinkPartsFactory(
        metricClientProvider, jobContext, ((ClickHouseSinkDto.Config) commandConfig));
  }
}
