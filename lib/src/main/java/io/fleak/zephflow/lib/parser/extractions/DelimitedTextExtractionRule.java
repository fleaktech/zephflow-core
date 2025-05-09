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
package io.fleak.zephflow.lib.parser.extractions;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

/** Created by bolei on 5/8/25 */
public class DelimitedTextExtractionRule implements ExtractionRule {
  private final String separator;
  private final ArrayList<String> columns;

  public DelimitedTextExtractionRule(@NonNull String separator, @NonNull List<String> columns) {
    this.separator = separator;
    this.columns = new ArrayList<>(columns);
  }

  @Override
  public RecordFleakData extract(String raw) throws Exception {
    if (StringUtils.isEmpty(raw)) {
      return new RecordFleakData();
    }
    String[] valueArr = raw.split(separator);
    if (valueArr.length > columns.size()) {
      throw new IllegalArgumentException("more values are provided than columns");
    }
    Map<String, String> payload = new HashMap<>();
    for (int i = 0; i < valueArr.length; ++i) {
      payload.put(columns.get(i), valueArr[i]);
    }
    return (RecordFleakData) FleakData.wrap(payload);
  }
}
