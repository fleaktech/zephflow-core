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
package io.fleak.zephflow.lib.serdes.decode.xml;

import com.ctc.wstx.api.WstxInputProperties;
import com.ctc.wstx.stax.WstxInputFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.xml.XmlFactory;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.IOException;
import java.io.InputStream;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * Woodstox is named explicitly: other StAX implementations on the classpath (a shaded copy in the
 * Hadoop client, for one) would otherwise be picked by service lookup. DTDs are refused outright,
 * so no entity is expanded and nothing is fetched.
 */
final class XmlSupport {

  private static final XMLInputFactory INPUT = inputFactory();

  private static final XmlMapper MAPPER =
      new XmlMapper(
          XmlFactory.builder()
              .xmlInputFactory(INPUT)
              .streamReadConstraints(
                  StreamReadConstraints.builder()
                      .maxStringLength(Integer.MAX_VALUE)
                      .maxNameLength(Integer.MAX_VALUE)
                      .build())
              .build());

  private XmlSupport() {}

  private static XMLInputFactory inputFactory() {
    XMLInputFactory factory = new WstxInputFactory();
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
    factory.setProperty(WstxInputProperties.P_MAX_ATTRIBUTE_SIZE, Integer.MAX_VALUE);
    return factory;
  }

  static XMLStreamReader reader(InputStream in) throws XMLStreamException {
    return INPUT.createXMLStreamReader(in);
  }

  /** Reads the element the reader is positioned on; the reader ends on its END_ELEMENT. */
  static RecordFleakData readElement(XMLStreamReader reader) throws IOException {
    JsonNode node = MAPPER.readTree(MAPPER.getFactory().createParser(reader));
    if (!(node instanceof ObjectNode object)) {
      throw new MalformedXmlException("element did not map to an object");
    }
    return JsonUtils.fromJsonPayload(object);
  }

  static void rejectDtd(int event) throws MalformedXmlException {
    if (event == XMLStreamConstants.DTD) {
      throw new MalformedXmlException("DTDs are not allowed");
    }
  }
}
