package io.fleak.zephflow.lib.commands.clickhouse;

import io.fleak.zephflow.api.CommandConfig;
import lombok.*;

public class ClickHouseSinkDto {

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Config implements CommandConfig {
    private String username;
    private String password;
    @Builder.Default private String clientName = "zephflow";
    private String credentialId;
    @NonNull private String database;
    @NonNull private String table;
    @NonNull private String endpoint;
  }
}
