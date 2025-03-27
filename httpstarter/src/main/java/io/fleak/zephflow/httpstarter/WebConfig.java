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
package io.fleak.zephflow.httpstarter;

import static org.springframework.http.MediaType.APPLICATION_JSON;

import io.fleak.zephflow.httpstarter.converters.EventsReqHttpMessageConverter;
import io.fleak.zephflow.lib.serdes.des.csv.CsvDeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.jsonarr.JsonArrayDeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.strline.StringLineDeserializerFactory;
import java.util.List;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/** Created by bolei on 3/30/24 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

  private final CsvDeserializerFactory csvDeserializerFactory;
  private final JsonArrayDeserializerFactory jsonArrayDeserializerFactory;
  private final StringLineDeserializerFactory stringLineDeserializerFactory;

  public WebConfig(
      CsvDeserializerFactory csvDeserializerFactory,
      JsonArrayDeserializerFactory jsonArrayDeserializerFactory,
      StringLineDeserializerFactory stringLineDeserializerFactory) {
    this.csvDeserializerFactory = csvDeserializerFactory;
    this.jsonArrayDeserializerFactory = jsonArrayDeserializerFactory;
    this.stringLineDeserializerFactory = stringLineDeserializerFactory;
  }

  @Override
  public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
    // always addFirst to make sure these converters take precedence over default ones.
    converters.addFirst(
        new EventsReqHttpMessageConverter(
            APPLICATION_JSON, jsonArrayDeserializerFactory.createDeserializer()));
    converters.addFirst(
        new EventsReqHttpMessageConverter(
            new MediaType("text", "csv"), csvDeserializerFactory.createDeserializer()));
    converters.addFirst(
        new EventsReqHttpMessageConverter(
            new MediaType("text", "plain"), stringLineDeserializerFactory.createDeserializer()));
  }
}
