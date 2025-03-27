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
package io.fleak.zephflow.lib;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_ENV;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_SERVICE;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUtils {

  public static final UsernamePasswordCredential USERNAME_PASSWORD_CREDENTIAL =
      new UsernamePasswordCredential("MY_USER_NAME", "MY_PASSWORD");

  public static final JobContext JOB_CONTEXT = buildJobContext();

  public static JobContext buildJobContext() {
    return JobContext.builder()
        .otherProperties(
            new HashMap<>(
                Map.of(
                    "credential_2",
                    OBJECT_MAPPER.convertValue(
                        USERNAME_PASSWORD_CREDENTIAL, new TypeReference<>() {}))))
        .metricTags(
            Map.of(
                METRIC_TAG_SERVICE, "my_service",
                METRIC_TAG_ENV, "my_env"))
        .build();
  }

  public static class TestSourceEventAcceptor implements SourceEventAcceptor {
    @Override
    public void terminate() {
      // no-op
    }

    @Override
    public void accept(List<RecordFleakData> sourceEvents) {
      sourceEvents.stream()
          .map(RecordFleakData::unwrap)
          .map(JsonUtils::toJsonString)
          .forEach(System.out::println);
    }
  }
}
