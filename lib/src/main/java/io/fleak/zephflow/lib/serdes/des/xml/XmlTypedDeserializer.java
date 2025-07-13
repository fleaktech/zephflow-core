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
package io.fleak.zephflow.lib.serdes.des.xml;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.lib.serdes.des.SingleEventTypedDeserializer;
import io.fleak.zephflow.lib.utils.XmlUtils;
import java.util.Map;

/** Created by bolei on 7/11/25 */
public class XmlTypedDeserializer extends SingleEventTypedDeserializer<Map<String, Object>> {
  @Override
  protected Map<String, Object> deserializeOne(byte[] value) throws Exception {
    return XmlUtils.XML_MAPPER.readValue(value, new TypeReference<>() {});
  }
}
