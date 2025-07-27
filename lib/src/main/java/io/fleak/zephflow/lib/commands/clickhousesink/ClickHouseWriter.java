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
package io.fleak.zephflow.lib.commands.clickhousesink;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.*;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickHouseWriter implements SimpleSinkCommand.Flusher<Map<String, Object>> {

  private final Client client;
  private TableSchema tableSchema;
  private ClickHouseFormat clickHouseFormat;

  public ClickHouseWriter(
      ClickHouseSinkDto.Config config, @Nullable UsernamePasswordCredential credentials) {
    var clientBuilder =
        new Client.Builder()
            .addEndpoint(config.getEndpoint())
            .setClientName(config.getClientName())
            .disableNativeCompression(config.isDisableNativeCompression())
            .compressServerResponse(config.isCompressServerResponse())
            .compressClientRequest(config.isCompressClientRequest())
            .setDefaultDatabase(config.getDatabase());

    if (credentials != null) {
      clientBuilder.setUsername(credentials.getUsername());
      clientBuilder.setPassword(credentials.getPassword());
    }

    config.serverSettings.forEach(
        (k, v) -> {
          if (v != null) {
            if (v instanceof Collection) {
              try {
                //noinspection unchecked
                clientBuilder.serverSetting(k, (Collection<String>) v);
              } catch (ClassCastException e) {
                throw new ClassCastException(
                    "Server setting " + k + " is not a collection of type string");
              }
            } else {
              clientBuilder.serverSetting(k, v.toString());
            }
          }
        });
    this.client = clientBuilder.build();
  }

  public void downloadAndSetSchema(String db, String table) {
    this.tableSchema = client.getTableSchema(table, db);
    clickHouseFormat =
        tableSchema.hasDefaults()
            ? ClickHouseFormat.RowBinaryWithDefaults
            : ClickHouseFormat.RowBinary;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> preparedInputEvents)
      throws Exception {
    if (tableSchema == null) {
      throw new IOException("First register schema is required");
    }

    var table = tableSchema.getTableName();
    var data = preparedInputEvents.preparedList();

    var out = new ByteArrayOutputStream();
    var writer = new RowBinaryFormatWriter(out, tableSchema, clickHouseFormat);

    for (Map<String, Object> dataItem : data) {
      for (var column : tableSchema.getColumns()) {
        var val = dataItem.get(column.getColumnName());
        // Type helpers, till we have Eval LocalDate and Date Parsing support
        // convert Long to LocalDate
        if (val != null
            && column.getDataType() == ClickHouseDataType.Date
            && val instanceof Long v) {
          val = Instant.ofEpochMilli(v).atZone(ZoneOffset.UTC).toLocalDate();
        }
        writer.setValue(column.getColumnName(), val);
      }
      writer.commitRow();
    }

    var input = new ByteArrayInputStream(out.toByteArray());

    try (var response =
        client.insert(table, input, writer.getFormat(), new InsertSettings()).get()) {
      log.trace("Successfully wrote to {} {}", tableSchema, response);
      return new SimpleSinkCommand.FlushResult(
          (int) response.getWrittenRows(), response.getWrittenBytes(), List.of());
    } catch (Exception e) {
      log.error("Error writing clickhouse data to {}", tableSchema, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    client.close();
  }
}
