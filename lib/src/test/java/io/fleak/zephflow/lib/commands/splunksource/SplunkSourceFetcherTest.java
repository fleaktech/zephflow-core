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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.splunk.Job;
import com.splunk.JobCollection;
import com.splunk.JobResultsArgs;
import com.splunk.Service;
import io.fleak.zephflow.lib.commands.source.NoCommitStrategy;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.junit.jupiter.api.Test;

class SplunkSourceFetcherTest {

  @Test
  void testSplunkSourceFetcherPollingBehavior() throws Exception {
    var config =
        SplunkSourceDto.Config.builder()
            .splunkUrl("https://splunk.example.com:8089")
            .searchQuery("search index=main | head 5")
            .credentialId("splunk-cred")
            .build();

    Service mockService = mock(Service.class);
    JobCollection mockJobCollection = mock(JobCollection.class);
    Job mockJob = mock(Job.class);

    when(mockService.getJobs()).thenReturn(mockJobCollection);
    when(mockJobCollection.create("search index=main | head 5")).thenReturn(mockJob);
    when(mockJob.getSid()).thenReturn("search-job-123");
    when(mockJob.isDone()).thenReturn(false, false, true);

    String jsonResults =
        "{\"preview\":false,\"init_offset\":0,\"messages\":[],\"fields\":[{\"name\":\"host\"},{\"name\":\"_raw\"}],\"results\":[{\"host\":\"server1\",\"_raw\":\"Event 1\"}]}";
    InputStream resultsStream = new ByteArrayInputStream(jsonResults.getBytes());
    when(mockJob.getResults(any(JobResultsArgs.class))).thenReturn(resultsStream);

    var fetcher = new SplunkSourceFetcher(config, mockService);

    var results = fetcher.fetch();

    assertNotNull(results);

    verify(mockJob, times(1)).cancel();
    verify(mockJob, times(3)).isDone();
    verify(mockJob, times(2)).refresh();
    verify(mockJobCollection, times(1)).create("search index=main | head 5");
  }

  @Test
  void testSplunkSourceFetcherCommitStrategy() {
    var config =
        SplunkSourceDto.Config.builder()
            .splunkUrl("https://splunk.example.com:8089")
            .searchQuery("search index=main")
            .credentialId("splunk-cred")
            .build();

    Service mockService = mock(Service.class);
    var fetcher = new SplunkSourceFetcher(config, mockService);

    assertEquals(NoCommitStrategy.INSTANCE, fetcher.commitStrategy());
  }

  @Test
  void testSplunkSourceFetcherClose() throws Exception {
    var config =
        SplunkSourceDto.Config.builder()
            .splunkUrl("https://splunk.example.com:8089")
            .searchQuery("search index=main")
            .credentialId("splunk-cred")
            .build();

    Service mockService = mock(Service.class);
    var fetcher = new SplunkSourceFetcher(config, mockService);

    fetcher.close();

    verify(mockService, times(1)).logout();
  }
}
