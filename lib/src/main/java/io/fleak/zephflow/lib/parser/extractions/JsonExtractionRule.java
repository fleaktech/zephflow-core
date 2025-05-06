package io.fleak.zephflow.lib.parser.extractions;

import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonString;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.structure.RecordFleakData;

/** Created by bolei on 5/6/25 */
public class JsonExtractionRule implements ExtractionRule{
  @Override
  public RecordFleakData extract(String raw) throws Exception {
    return fromJsonString(raw, new TypeReference<>() {});
  }
}
