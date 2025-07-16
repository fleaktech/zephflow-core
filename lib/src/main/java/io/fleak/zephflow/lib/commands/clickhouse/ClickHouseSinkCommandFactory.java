package io.fleak.zephflow.lib.commands.clickhouse;

import io.fleak.zephflow.api.CommandFactory;
import io.fleak.zephflow.api.CommandType;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.lib.commands.JsonConfigParser;

public class ClickHouseSinkCommandFactory extends CommandFactory {
  @Override
  public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
    JsonConfigParser<ClickHouseSinkDto.Config> configParser =
        new JsonConfigParser<>(ClickHouseSinkDto.Config.class);
    ClickHouseConfigValidator validator = new ClickHouseConfigValidator();
    ClickHouseSinkCommandInitializerFactory initializerFactory =
        new ClickHouseSinkCommandInitializerFactory();
    return new ClickHouseSinkCommand(
        nodeId, jobContext, configParser, validator, initializerFactory);
  }

  @Override
  public CommandType commandType() {
    return CommandType.SINK;
  }
}
