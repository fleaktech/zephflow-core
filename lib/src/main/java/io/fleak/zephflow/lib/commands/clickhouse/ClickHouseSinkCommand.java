package io.fleak.zephflow.lib.commands.clickhouse;

import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_CLICK_HOUSE_SINK;

import io.fleak.zephflow.api.ConfigParser;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.util.Map;

public class ClickHouseSinkCommand extends SimpleSinkCommand<Map<String, Object>> {

  private static final int CLICKHOUSE_SINK_BATCH_SIZE = 100;

  protected ClickHouseSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      ClickHouseSinkCommandInitializerFactory snowflakeSinkCommandInitializerFactory) {
    super(
        nodeId, jobContext, configParser, configValidator, snowflakeSinkCommandInitializerFactory);
  }

  @Override
  protected int batchSize() {
    return CLICKHOUSE_SINK_BATCH_SIZE;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_CLICK_HOUSE_SINK;
  }
}
