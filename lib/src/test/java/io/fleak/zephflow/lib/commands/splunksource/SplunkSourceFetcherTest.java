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
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SplunkSourceFetcherTest {

  @Test
  void testSplunkSourceFetcherPollingBehavior() throws Exception {
    var config =
        SplunkSourceDto.Config.builder()
            .splunkUrl("https://splunk.example.com:8089")
            .searchQuery("search index=main | head 5")
            .credentialId("splunk-cred")
            .batchSize(100)
            .build();

    Service mockService = mock(Service.class);
    JobCollection mockJobCollection = mock(JobCollection.class);
    Job mockJob = mock(Job.class);

    when(mockService.getJobs()).thenReturn(mockJobCollection);
    when(mockJobCollection.create("search index=main | head 5")).thenReturn(mockJob);
    when(mockJob.getSid()).thenReturn("search-job-123");
    when(mockJob.isDone()).thenReturn(false, false, true);
    when(mockJob.getResultCount()).thenReturn(1);

    String jsonResults =
        "{\"preview\":false,\"init_offset\":0,\"messages\":[],\"fields\":[{\"name\":\"host\"},{\"name\":\"_raw\"}],\"results\":[{\"host\":\"server1\",\"_raw\":\"Event 1\"}]}";
    InputStream resultsStream = new ByteArrayInputStream(jsonResults.getBytes());
    when(mockJob.getResults(any(JobResultsArgs.class))).thenReturn(resultsStream);

    var fetcher = new SplunkSourceFetcher(config, mockService);

    var results = fetcher.fetch();

    assertNotNull(results);
    assertEquals(1, results.size());

    verify(mockJob, times(3)).isDone();
    verify(mockJob, times(2)).refresh();
    verify(mockJob, times(1)).getResultCount();
    verify(mockJobCollection, times(1)).create("search index=main | head 5");
  }

  @Test
  void testPaginationMultipleBatches() throws Exception {
    var config =
        SplunkSourceDto.Config.builder()
            .splunkUrl("https://splunk.example.com:8089")
            .searchQuery("search index=main")
            .credentialId("splunk-cred")
            .batchSize(2)
            .build();

    Service mockService = mock(Service.class);
    JobCollection mockJobCollection = mock(JobCollection.class);
    Job mockJob = mock(Job.class);

    when(mockService.getJobs()).thenReturn(mockJobCollection);
    when(mockJobCollection.create("search index=main")).thenReturn(mockJob);
    when(mockJob.getSid()).thenReturn("search-job-123");
    when(mockJob.isDone()).thenReturn(true);
    when(mockJob.getResultCount()).thenReturn(3);

    String batch1Json =
        "{\"preview\":false,\"init_offset\":0,\"messages\":[],\"fields\":[{\"name\":\"host\"}],\"results\":[{\"host\":\"server1\"},{\"host\":\"server2\"}]}";
    String batch2Json =
        "{\"preview\":false,\"init_offset\":0,\"messages\":[],\"fields\":[{\"name\":\"host\"}],\"results\":[{\"host\":\"server3\"}]}";
    String emptyJson =
        "{\"preview\":false,\"init_offset\":0,\"messages\":[],\"fields\":[{\"name\":\"host\"}],\"results\":[]}";

    when(mockJob.getResults(any(JobResultsArgs.class)))
        .thenReturn(new ByteArrayInputStream(batch1Json.getBytes()))
        .thenReturn(new ByteArrayInputStream(batch2Json.getBytes()))
        .thenReturn(new ByteArrayInputStream(emptyJson.getBytes()));

    var fetcher = new SplunkSourceFetcher(config, mockService);

    assertFalse(fetcher.isExhausted());

    List<Map<String, String>> batch1 = fetcher.fetch();
    assertEquals(2, batch1.size());
    assertFalse(fetcher.isExhausted());

    List<Map<String, String>> batch2 = fetcher.fetch();
    assertEquals(1, batch2.size());
    assertTrue(fetcher.isExhausted());

    List<Map<String, String>> batch3 = fetcher.fetch();
    assertTrue(batch3.isEmpty());

    verify(mockJobCollection, times(1)).create("search index=main");
  }

  @Test
  void testExhaustedWithZeroResults() throws Exception {
    var config =
        SplunkSourceDto.Config.builder()
            .splunkUrl("https://splunk.example.com:8089")
            .searchQuery("search index=main | head 0")
            .credentialId("splunk-cred")
            .build();

    Service mockService = mock(Service.class);
    JobCollection mockJobCollection = mock(JobCollection.class);
    Job mockJob = mock(Job.class);

    when(mockService.getJobs()).thenReturn(mockJobCollection);
    when(mockJobCollection.create("search index=main | head 0")).thenReturn(mockJob);
    when(mockJob.getSid()).thenReturn("search-job-123");
    when(mockJob.isDone()).thenReturn(true);
    when(mockJob.getResultCount()).thenReturn(0);

    String emptyJson =
        "{\"preview\":false,\"init_offset\":0,\"messages\":[],\"fields\":[],\"results\":[]}";
    when(mockJob.getResults(any(JobResultsArgs.class)))
        .thenReturn(new ByteArrayInputStream(emptyJson.getBytes()));

    var fetcher = new SplunkSourceFetcher(config, mockService);

    assertFalse(fetcher.isExhausted());

    List<Map<String, String>> results = fetcher.fetch();
    assertTrue(results.isEmpty());
    assertTrue(fetcher.isExhausted());
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
  void testCloseWithoutFetch() throws Exception {
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

  @Test
  void testCloseAfterFetch() throws Exception {
    var config =
        SplunkSourceDto.Config.builder()
            .splunkUrl("https://splunk.example.com:8089")
            .searchQuery("search index=main")
            .credentialId("splunk-cred")
            .build();

    Service mockService = mock(Service.class);
    JobCollection mockJobCollection = mock(JobCollection.class);
    Job mockJob = mock(Job.class);

    when(mockService.getJobs()).thenReturn(mockJobCollection);
    when(mockJobCollection.create("search index=main")).thenReturn(mockJob);
    when(mockJob.getSid()).thenReturn("search-job-123");
    when(mockJob.isDone()).thenReturn(true);
    when(mockJob.getResultCount()).thenReturn(0);

    String emptyJson =
        "{\"preview\":false,\"init_offset\":0,\"messages\":[],\"fields\":[],\"results\":[]}";
    when(mockJob.getResults(any(JobResultsArgs.class)))
        .thenReturn(new ByteArrayInputStream(emptyJson.getBytes()));

    var fetcher = new SplunkSourceFetcher(config, mockService);
    fetcher.fetch();
    fetcher.close();

    verify(mockJob, times(1)).cancel();
    verify(mockService, times(1)).logout();
  }

  @Test
  void testJobInitializationTimeout() {
    var config =
        SplunkSourceDto.Config.builder()
            .splunkUrl("https://splunk.example.com:8089")
            .searchQuery("search index=main | head 5")
            .credentialId("splunk-cred")
            .batchSize(100)
            .jobInitTimeoutMs(1500L)
            .build();

    Service mockService = mock(Service.class);
    JobCollection mockJobCollection = mock(JobCollection.class);
    Job mockJob = mock(Job.class);

    when(mockService.getJobs()).thenReturn(mockJobCollection);
    when(mockJobCollection.create("search index=main | head 5")).thenReturn(mockJob);
    when(mockJob.getSid()).thenReturn("search-job-timeout-test");
    when(mockJob.isDone()).thenReturn(false);

    var fetcher = new SplunkSourceFetcher(config, mockService);

    var exception = assertThrows(RuntimeException.class, fetcher::fetch);
    assertInstanceOf(SplunkSourceFetcherError.class, exception.getCause());
    assertTrue(exception.getCause().getMessage().contains("timed out"));
    assertTrue(exception.getCause().getMessage().contains("search-job-timeout-test"));

    verify(mockJob).cancel();
  }

  @Test
  void testNoTimeoutWhenZero() throws Exception {
    var config =
        SplunkSourceDto.Config.builder()
            .splunkUrl("https://splunk.example.com:8089")
            .searchQuery("search index=main | head 5")
            .credentialId("splunk-cred")
            .batchSize(100)
            .jobInitTimeoutMs(0L)
            .build();

    Service mockService = mock(Service.class);
    JobCollection mockJobCollection = mock(JobCollection.class);
    Job mockJob = mock(Job.class);

    when(mockService.getJobs()).thenReturn(mockJobCollection);
    when(mockJobCollection.create("search index=main | head 5")).thenReturn(mockJob);
    when(mockJob.getSid()).thenReturn("search-job-123");
    when(mockJob.isDone()).thenReturn(false, false, true);
    when(mockJob.getResultCount()).thenReturn(1);

    String jsonResults =
        "{\"preview\":false,\"init_offset\":0,\"messages\":[],\"fields\":[{\"name\":\"host\"}],\"results\":[{\"host\":\"server1\"}]}";
    when(mockJob.getResults(any(JobResultsArgs.class)))
        .thenReturn(new ByteArrayInputStream(jsonResults.getBytes()));

    var fetcher = new SplunkSourceFetcher(config, mockService);
    var results = fetcher.fetch();

    assertNotNull(results);
    assertEquals(1, results.size());
    verify(mockJob, times(3)).isDone();
  }
}
