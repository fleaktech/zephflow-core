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
package io.fleak.zephflow.lib.parser;

import static io.fleak.zephflow.lib.parser.extractions.WindowsMultilineExtractionRule.createTimestampExtractor;

import io.fleak.zephflow.lib.parser.extractions.*;
import io.fleak.zephflow.lib.parser.extractions.SyslogExtractionConfig;
import io.fleak.zephflow.lib.parser.extractions.SyslogExtractionRule;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.grok.Grok;

/** Created by bolei on 10/14/24 */
@Slf4j
public class ParserConfigCompiler {

  public CompiledRules.ParseRule compile(ParserConfigs.ParserConfig parserConfig) {
    if (parserConfig == null) {
      return null;
    }
    ExtractionRule extractionRule = compileExtractionConfig(parserConfig.getExtractionConfig());
    CompiledRules.DispatchRule dispatchRule =
        compileDispatchConfig(parserConfig.getDispatchConfig());
    return new CompiledRules.ParseRule(
        parserConfig.getTargetField(),
        parserConfig.isRemoveTargetField(),
        extractionRule,
        dispatchRule);
  }

  private CompiledRules.DispatchRule compileDispatchConfig(
      ParserConfigs.DispatchConfig dispatchConfig) {
    if (dispatchConfig == null) {
      return null;
    }
    Map<String, CompiledRules.ParseRule> dispatchRulesMap =
        dispatchConfig.getDispatchMap().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> compile(e.getValue())));

    CompiledRules.ParseRule defaultRule = compile(dispatchConfig.getDefaultConfig());

    return new CompiledRules.DispatchRule(
        dispatchConfig.getDispatchField(), dispatchRulesMap, defaultRule);
  }

  private ExtractionRule compileExtractionConfig(ExtractionConfig extractionConfig) {
    if (extractionConfig instanceof GrokExtractionConfig grokExtractionConfig) {
      Grok grok =
          new Grok(Grok.BUILTIN_PATTERNS, grokExtractionConfig.getGrokExpression(), log::warn);
      return new GrokExtractionRule(grokExtractionConfig.getGrokExpression(), grok);
    }
    if (extractionConfig
        instanceof WindowsMultilineExtractionConfig windowsMultilineExtractionConfig) {
      TimestampExtractor timestampExtractor =
          createTimestampExtractor(windowsMultilineExtractionConfig);
      return new WindowsMultilineExtractionRule(timestampExtractor);
    }

    if (extractionConfig instanceof SyslogExtractionConfig syslogExtractionConfig) {
      return new SyslogExtractionRule(syslogExtractionConfig);
    }

    if (extractionConfig instanceof CefExtractionConfig) {
      return new CefExtractionRule();
    }

    if (extractionConfig instanceof PanwTrafficExtractionConfig) {
      return new PanwTrafficExtractionRule();
    }

    if (extractionConfig instanceof JsonExtractionConfig jsonExtractionConfig) {
      return new JsonExtractionRule(jsonExtractionConfig);
    }

    if (extractionConfig instanceof DelimitedTextExtractionConfig delimitedTextExtractionConfig) {
      return new DelimitedTextExtractionRule(
          delimitedTextExtractionConfig.getDelimiter(), delimitedTextExtractionConfig.getColumns());
    }

    throw new IllegalArgumentException("unsupported extraction config: " + extractionConfig);
  }
}
