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
package io.fleak.zephflow.lib.commands.clickhouse;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseFormat;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ClickHouseWriter implements SimpleSinkCommand.Flusher<Map<String, Object>> {

  private final Client client;
  private TableSchema tableSchema;
  private ClickHouseSinkDto.Config config;

  public ClickHouseWriter(ClickHouseSinkDto.Config config, UsernamePasswordCredential credentials) {
    var clientBuilder =
        new Client.Builder()
            .addEndpoint(config.getEndpoint())
            .setClientName(config.getClientName())
            .disableNativeCompression(config.isDisableNativeCompression())
            .setUsername(credentials.getUsername())
            .setPassword(credentials.getPassword())
            .compressServerResponse(config.isCompressServerResponse())
            .setDefaultDatabase(config.getDatabase());

    config.serverSettings.forEach(
        (k, v) -> {
          if (v != null) {
            if (v instanceof Collection) {
              clientBuilder.serverSetting(k, (Collection<String>) v);
            } else {
              clientBuilder.serverSetting(k, v.toString());
            }
          }
        });

    this.client = clientBuilder.build();
    this.config = config;
  }

  public TableSchema registerSchema(String db, String table) {
    this.tableSchema = client.getTableSchema(table, db);
    return tableSchema;
  }

  public Client getClient() {
    return client;
  }

  private SimpleSinkCommand.FlushResult write(String table, List<Map<String, Object>> data)
      throws IOException, ExecutionException, InterruptedException {
    if (tableSchema == null) {
      throw new IOException("First register schema is required");
    }

    var out = new ByteArrayOutputStream();

    var writer =
        new RowBinaryFormatWriter(
            out,
            tableSchema,
            tableSchema.hasDefaults()
                ? ClickHouseFormat.RowBinaryWithDefaults
                : ClickHouseFormat.RowBinary);

    for (Map<String, Object> dataItem : data) {
      for (var column : tableSchema.getColumns()) {
        var val = dataItem.get(column.getColumnName());
        writer.setValue(column.getColumnName(), val);
      }
      writer.commitRow();
    }

    out.close();
    var input = new ByteArrayInputStream(out.toByteArray());

    try (var response =
        client.insert(table, input, writer.getFormat(), new InsertSettings()).get()) {
      return new SimpleSinkCommand.FlushResult(
          (int) response.getWrittenRows(), response.getWrittenBytes(), List.of());
    } finally {
      out.close();
    }
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> preparedInputEvents)
      throws Exception {
    return write(tableSchema.getTableName(), preparedInputEvents.preparedList());
  }

  @Override
  public void close() throws IOException {
    client.close();
  }
}
