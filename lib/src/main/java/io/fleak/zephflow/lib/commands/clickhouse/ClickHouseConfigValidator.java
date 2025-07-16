package io.fleak.zephflow.lib.commands.clickhouse;

import static io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredentialOpt;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;

public class ClickHouseConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    ClickHouseSinkDto.Config config = (ClickHouseSinkDto.Config) commandConfig;
    var usernamePasswordCredential =
        lookupUsernamePasswordCredentialOpt(jobContext, config.getCredentialId());
    if (config.getCredentialId() != null && !(usernamePasswordCredential.isPresent())) {
      throw new RuntimeException(
          "The credentialId is specific but no credentials record was found");
    }

    var userName = Strings.trimToNull(config.getUsername());

    if (config.getCredentialId() == null && userName == null) {
      throw new RuntimeException("A username and password must be specified");
    }

    Objects.requireNonNull(
        StringUtils.trimToNull(config.getEndpoint()), "A clickhouse endpoint must be specified");
    Objects.requireNonNull(
        StringUtils.trimToNull(config.getDatabase()), "A clickhouse database must be specified");
    Objects.requireNonNull(
        StringUtils.trimToNull(config.getTable()), "A clickhouse table must be specified");
  }
}
