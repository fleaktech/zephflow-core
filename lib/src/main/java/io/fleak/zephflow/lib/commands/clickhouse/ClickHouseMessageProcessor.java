package io.fleak.zephflow.lib.commands.clickhouse;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.util.Map;

public class ClickHouseMessageProcessor
    implements SimpleSinkCommand.SinkMessagePreProcessor<Map<String, Object>> {
  @Override
  public Map<String, Object> preprocess(RecordFleakData event, long ts) {
    return event.unwrap();
  }
}
