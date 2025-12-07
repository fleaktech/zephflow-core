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

import static io.fleak.zephflow.lib.utils.MiscUtils.threadSleep;

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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
@RequiredArgsConstructor
public class SplunkSourceFetcher implements Fetcher<Map<String, String>> {

  private static final int POLL_INTERVAL_MS = 1000;

  private final SplunkSourceDto.Config config;
  private final Service service;

  private Job job;
  private int currentOffset = 0;
  private long totalResultCount = -1;

  @Override
  public List<Map<String, String>> fetch() {
    try {
      if (job == null) {
        initializeJob();
      }

      if (isExhausted()) {
        return List.of();
      }

      log.debug("Fetching batch at offset {} (total: {})", currentOffset, totalResultCount);
      JobResultsArgs resultsArgs = new JobResultsArgs();
      resultsArgs.setOffset(currentOffset);
      resultsArgs.setCount(config.getBatchSize());
      resultsArgs.setOutputMode(JobResultsArgs.OutputMode.JSON);

      List<Map<String, String>> results = new ArrayList<>();
      try (InputStream resultsStream = job.getResults(resultsArgs)) {
        ResultsReaderJson reader = new ResultsReaderJson(resultsStream);
        HashMap<String, String> event;
        while ((event = reader.getNextEvent()) != null) {
          results.add(new HashMap<>(event));
        }
      }

      currentOffset += results.size();
      log.info("Fetched {} events from Splunk (offset now: {})", results.size(), currentOffset);
      return results;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Splunk search job polling was interrupted", e);
    } catch (Exception e) {
      throw new RuntimeException("Failed to fetch data from Splunk", e);
    }
  }

  private void initializeJob() throws InterruptedException {
    log.info("Creating Splunk search job for query: {}", config.getSearchQuery());

    JobArgs jobArgs = new JobArgs();
    if (StringUtils.isNotBlank(config.getEarliestTime())) {
      jobArgs.setEarliestTime(config.getEarliestTime());
    }
    if (StringUtils.isNotBlank(config.getLatestTime())) {
      jobArgs.setLatestTime(config.getLatestTime());
    }

    job = service.getJobs().create(config.getSearchQuery(), jobArgs);

    log.info("Polling for job completion (job ID: {})", job.getSid());
    long timeoutMs = config.getJobInitTimeoutMs();
    long startTime = System.currentTimeMillis();

    while (!job.isDone()) {
      if (timeoutMs > 0 && System.currentTimeMillis() - startTime >= timeoutMs) {
        job.cancel();
        throw new SplunkSourceFetcherError(
            String.format(
                "Splunk job timed out after %d ms (job ID: %s)", timeoutMs, job.getSid()));
      }
      threadSleep(POLL_INTERVAL_MS);
      job.refresh();
    }

    totalResultCount = job.getResultCount();
    log.info("Job completed. Total result count: {}", totalResultCount);
  }

  @Override
  public boolean isExhausted() {
    return totalResultCount >= 0 && currentOffset >= totalResultCount;
  }

  @Override
  public CommitStrategy commitStrategy() {
    return NoCommitStrategy.INSTANCE;
  }

  @Override
  public void close() throws IOException {
    if (job != null) {
      job.cancel();
    }
    if (service != null) {
      service.logout();
    }
  }
}
