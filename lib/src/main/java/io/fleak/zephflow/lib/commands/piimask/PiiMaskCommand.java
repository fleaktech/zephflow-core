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
package io.fleak.zephflow.lib.commands.piimask;

import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_PII_MASK;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_ERROR_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_OUTPUT_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.basicCommandMetricTags;
import static io.fleak.zephflow.lib.utils.MiscUtils.getCallingUserTagAndEventTags;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigParser;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.ExecutionContext;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.ScalarCommand;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Config;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.CustomPattern;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.DetectorConfig;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Detectors;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class PiiMaskCommand extends ScalarCommand {

  public static final String METRIC_NAME_PII_MATCH_COUNT = "pii_match_count";

  protected PiiMaskCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator) {
    super(nodeId, jobContext, configParser, configValidator);
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    Map<String, String> metricTags =
        basicCommandMetricTags(jobContext.getMetricTags(), commandName(), nodeId);
    FleakCounter input = metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_COUNT, metricTags);
    FleakCounter output = metricClientProvider.counter(METRIC_NAME_OUTPUT_EVENT_COUNT, metricTags);
    FleakCounter error = metricClientProvider.counter(METRIC_NAME_ERROR_EVENT_COUNT, metricTags);
    FleakCounter piiMatch = metricClientProvider.counter(METRIC_NAME_PII_MATCH_COUNT, metricTags);

    Config config = (Config) commandConfig;

    List<DottedPath> paths = new ArrayList<>();
    for (String t : config.targets()) {
      paths.add(DottedPath.parse(t));
    }

    List<PiiMasker.Spec> specs = new ArrayList<>();
    if (config.customPatterns() != null) {
      for (CustomPattern cp : config.customPatterns()) {
        specs.add(PiiMasker.custom(cp.name(), Pattern.compile(cp.pattern()), cp.replacement()));
      }
    }
    Detectors d = config.detectors();
    if (d != null) {
      for (BuiltInDetectors bd : BuiltInDetectors.values()) {
        DetectorConfig dc = d.get(bd);
        if (dc != null) specs.add(PiiMasker.builtIn(bd, dc.replacement()));
      }
    }

    return new PiiMaskExecutionContext(input, output, error, paths, specs, piiMatch);
  }

  @Override
  protected List<RecordFleakData> processOneEvent(
      RecordFleakData event, String callingUser, ExecutionContext context) {
    PiiMaskExecutionContext ctx = (PiiMaskExecutionContext) context;
    Map<String, String> tags = getCallingUserTagAndEventTags(callingUser, event);
    ctx.getInputMessageCounter().increase(tags);

    try {
      int totalReplacements = 0;
      for (DottedPath path : ctx.getTargets()) {
        totalReplacements +=
            PiiPathWriter.rewrite(event, path, s -> PiiMasker.mask(s, ctx.getSpecs()));
      }
      if (totalReplacements > 0) {
        ctx.getPiiMatchCounter().increase(totalReplacements, tags);
      }
      ctx.getOutputMessageCounter().increase(tags);
      return List.of(event);
    } catch (Exception e) {
      ctx.getErrorCounter().increase(tags);
      throw e;
    }
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_PII_MASK;
  }
}
