package io.fleak.zephflow.lib.serdes.des.xml;

import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.converters.ObjectMapTypedEventConverter;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import io.fleak.zephflow.lib.serdes.des.SingleEventDeserializer;
import java.util.Map;

/** Created by bolei on 7/11/25 */
public class XmlDeserializerFactory implements DeserializerFactory<Map<String, Object>> {
  @Override
  public FleakDeserializer<Map<String, Object>> createDeserializer() {
    var typedEventConverter = new ObjectMapTypedEventConverter();
    XmlTypedDeserializer xmlTypedDeserializer = new XmlTypedDeserializer();
    return new SingleEventDeserializer<>(
        EncodingType.XML, typedEventConverter, xmlTypedDeserializer);
  }
}
