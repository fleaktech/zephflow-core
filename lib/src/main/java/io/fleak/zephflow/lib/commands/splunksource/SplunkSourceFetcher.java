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
package io.fleak.zephflow.lib.commands.splunksource;

import com.splunk.*;
import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.NoCommitStrategy;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public record SplunkSourceFetcher(SplunkSourceDto.Config config, Service service)
    implements Fetcher<Map<String, String>> {

  private static final int POLL_INTERVAL_MS = 1000;

  @Override
  public List<Map<String, String>> fetch() {
    log.info("Creating Splunk search job for query: {}", config.getSearchQuery());
    Job job = service.getJobs().create(config.getSearchQuery());
    try {
      log.info("Polling for job completion (job ID: {})", job.getSid());
      while (!job.isDone()) {
        //noinspection BusyWait
        Thread.sleep(POLL_INTERVAL_MS);
        job.refresh();
      }

      log.info("Job completed. Fetching results...");
      JobResultsArgs resultsArgs = new JobResultsArgs();
      resultsArgs.setCount(0);
      resultsArgs.setOutputMode(JobResultsArgs.OutputMode.JSON);

      List<Map<String, String>> results = new ArrayList<>();
      try (InputStream resultsStream = job.getResults(resultsArgs)) {
        ResultsReaderJson reader = new ResultsReaderJson(resultsStream);
        HashMap<String, String> event;
        while ((event = reader.getNextEvent()) != null) {
          results.add(new HashMap<>(event));
        }
      }

      log.info("Fetched {} events from Splunk", results.size());
      return results;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Splunk search job polling was interrupted", e);
    } catch (Exception e) {
      throw new RuntimeException("Failed to fetch data from Splunk", e);
    } finally {
      job.cancel();
    }
  }

  @Override
  public CommitStrategy commitStrategy() {
    return NoCommitStrategy.INSTANCE;
  }

  @Override
  public void close() throws IOException {
    if (service != null) {
      service.logout();
    }
  }
}
