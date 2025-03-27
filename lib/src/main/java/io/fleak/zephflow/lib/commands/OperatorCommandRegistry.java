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

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.google.common.collect.ImmutableMap;
import io.fleak.zephflow.api.CommandFactory;
import io.fleak.zephflow.lib.commands.assertion.AssertionCommandFactory;
import io.fleak.zephflow.lib.commands.eval.EvalCommandFactory;
import io.fleak.zephflow.lib.commands.filesource.FileSourceCommandFactory;
import io.fleak.zephflow.lib.commands.kafkasink.KafkaSinkCommandFactory;
import io.fleak.zephflow.lib.commands.kafkasource.KafkaSourceCommandFactory;
import io.fleak.zephflow.lib.commands.kinesis.KinesisSinkCommandFactory;
import io.fleak.zephflow.lib.commands.noop.NoopCommandFactory;
import io.fleak.zephflow.lib.commands.parser.ParserCommandFactory;
import io.fleak.zephflow.lib.commands.s3.S3SinkCommandFactory;
import io.fleak.zephflow.lib.commands.sql.SqlCommandFactory;
import io.fleak.zephflow.lib.commands.stdin.StdInCommandFactory;
import io.fleak.zephflow.lib.commands.stdout.StdOutSinkCommandFactory;
import java.util.Map;

public interface OperatorCommandRegistry {

  Map<String, CommandFactory> OPERATOR_COMMANDS =
      ImmutableMap.<String, CommandFactory>builder()
          .put(COMMAND_NAME_NOOP, new NoopCommandFactory())
          .put(COMMAND_NAME_SQL_EVAL, new SqlCommandFactory())
          .put(COMMAND_NAME_S3_SINK, new S3SinkCommandFactory())
          .put(COMMAND_NAME_KINESIS_SINK, new KinesisSinkCommandFactory())
          .put(COMMAND_NAME_KAFKA_SOURCE, new KafkaSourceCommandFactory())
          .put(COMMAND_NAME_KAFKA_SINK, new KafkaSinkCommandFactory())
          .put(COMMAND_NAME_EVAL, new EvalCommandFactory())
          .put(COMMAND_NAME_ASSERTION, new AssertionCommandFactory(true))
          .put(COMMAND_NAME_FILTER, new AssertionCommandFactory(false))
          .put(COMMAND_NAME_STDIN, new StdInCommandFactory())
          .put(COMMAND_NAME_STDOUT, new StdOutSinkCommandFactory())
          .put(COMMAND_NAME_PARSER, new ParserCommandFactory())
          .put(COMMAND_NAME_FILE_SOURCE, new FileSourceCommandFactory())
          .build();
}
