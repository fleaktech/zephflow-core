package io.fleak.zephflow.lib.parser.extractions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Created by bolei on 8/11/25 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class KvPairExtractionConfig implements ExtractionConfig {
  private char pairSeparator;
  private char kvSeparator;
}
