package io.fleak.zephflow.lib.commands.clickhouse;

import static io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredentialOpt;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkCommandPartsFactory;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.util.Map;

public class ClickHouseSinkPartsFactory extends SinkCommandPartsFactory<Map<String, Object>> {

  private final ClickHouseSinkDto.Config config;

  protected ClickHouseSinkPartsFactory(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      ClickHouseSinkDto.Config config) {
    super(metricClientProvider, jobContext);
    this.config = config;
  }

  @Override
  public SimpleSinkCommand.Flusher<Map<String, Object>> createFlusher() {
    var writer = new ClickHouseWriter(config, getCredentials(config, jobContext));
    writer.registerSchema(config.getDatabase(), config.getTable());
    return writer;
  }

  private static UsernamePasswordCredential getCredentials(
      ClickHouseSinkDto.Config config, JobContext jobContext) {
    var usernamePasswordCredential =
        lookupUsernamePasswordCredentialOpt(jobContext, config.getCredentialId());
    return usernamePasswordCredential.orElseGet(
        () -> new UsernamePasswordCredential(config.getUsername(), config.getPassword()));
  }

  @Override
  public ClickHouseMessageProcessor createMessagePreProcessor() {
    return new ClickHouseMessageProcessor();
  }
}
