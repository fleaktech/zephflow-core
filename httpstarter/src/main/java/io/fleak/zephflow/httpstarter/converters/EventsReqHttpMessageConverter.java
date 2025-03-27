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
package io.fleak.zephflow.httpstarter.converters;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.httpstarter.dto.ExecuteDto;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.io.IOUtils;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

/** Created by bolei on 4/2/24 */
public class EventsReqHttpMessageConverter
    extends AbstractHttpMessageConverter<ExecuteDto.Request> {

  private final FleakDeserializer<?> fleakDeserializer;

  public EventsReqHttpMessageConverter(
      MediaType mediaType, FleakDeserializer<?> fleakDeserializer) {
    super(mediaType);
    this.fleakDeserializer = fleakDeserializer;
  }

  @Override
  protected boolean supports(@Nonnull Class<?> clazz) {
    return ExecuteDto.Request.class.isAssignableFrom(clazz);
  }

  @Nonnull
  @Override
  protected ExecuteDto.Request readInternal(
      @Nonnull Class<? extends ExecuteDto.Request> clazz, @Nonnull HttpInputMessage inputMessage)
      throws HttpMessageNotReadableException, IOException {
    byte[] raw = IOUtils.toByteArray(inputMessage.getBody());
    int byteCount = raw.length;
    List<RecordFleakData> recordFleakDataList;
    try {
      SerializedEvent serializedEvent = new SerializedEvent(null, raw, null);
      recordFleakDataList = fleakDeserializer.deserialize(serializedEvent);
    } catch (Exception e) {
      throw new HttpMessageNotReadableException(e.getMessage(), inputMessage);
    }
    return new ExecuteDto.Request(byteCount, recordFleakDataList);
  }

  @Override
  protected void writeInternal(
      @Nonnull ExecuteDto.Request recordFleakData, @Nonnull HttpOutputMessage outputMessage)
      throws HttpMessageNotWritableException {
    throw new UnsupportedOperationException("serializing ExecuteDto.Request not supported");
  }
}
