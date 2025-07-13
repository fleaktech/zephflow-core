package io.fleak.zephflow.lib.serdes.des.xml;

import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonResource;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

/** Created by bolei on 7/13/25 */
class XmlDeserializerFactoryTest {
  @Test
  void createDeserializer() throws Exception {
    byte[] raw;
    try (InputStream is = this.getClass().getResourceAsStream("/serdes/unittest_xml_input.xml")) {
      Preconditions.checkNotNull(is);
      raw = IOUtils.toByteArray(is);
    }
    var desFac = DeserializerFactory.createDeserializerFactory(EncodingType.XML);
    var deser = desFac.createDeserializer();
    List<RecordFleakData> actual = deser.deserialize(new SerializedEvent(null, raw, null));
    var expected =
        fromJsonResource(
            "/serdes/unittest_xml_expected_output.json", new TypeReference<Map<String, Object>>() {});
    assertEquals(1, actual.size());
    assertEquals(expected, actual.get(0).unwrap());
  }
}
