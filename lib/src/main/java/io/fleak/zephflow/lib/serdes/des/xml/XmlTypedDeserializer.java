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
