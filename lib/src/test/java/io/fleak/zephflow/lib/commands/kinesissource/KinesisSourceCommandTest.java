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
package io.fleak.zephflow.lib.commands.kinesissource;

import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.serdes.EncodingType;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.kinesis.common.InitialPositionInStream;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static org.junit.jupiter.api.Assertions.*;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.KINESIS;

@Slf4j
@Testcontainers
public class KinesisSourceCommandTest {

    private static final String STREAM_NAME = "test-stream-" + UUID.randomUUID();
    private static final String APPLICATION_NAME = "test-" + UUID.randomUUID();

    private static final StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(
            AwsBasicCredentials.create("test", "test")
    );

    public static LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
            .withServices(
                    KINESIS,
                    LocalStackContainer.Service.DYNAMODB,
                    LocalStackContainer.Service.CLOUDWATCH
            );

    private static KinesisClient kinesisClient;

    @Test
    public void testFetcher() throws Exception {
        var config = KinesisSourceDto.Config.builder()
                .encodingType(EncodingType.TEXT)
                .streamName(STREAM_NAME)
                .regionStr(LOCALSTACK.getRegion())
                .applicationName(APPLICATION_NAME)
                .cloudWatchEndpoint(new URI(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.CLOUDWATCH).toString()))
                .kinesisEndpoint(new URI(LOCALSTACK.getEndpointOverride(KINESIS).toString()))
                .dynamoEndpoint(new URI(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString()))
                .staticCredentials(new KinesisSourceDto.StaticCredentials(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()))
                .initialPosition(InitialPositionInStream.TRIM_HORIZON)
                .disableMetrics(true)
                .build();

        var commandFactory = new KinesisSourceCommandFactory();
        var command = commandFactory.createCommand("my_note", TestUtils.JOB_CONTEXT);

        var eventConsumer = new TestSourceEventAcceptor();

        command.parseAndValidateArg(toJsonString(config));

        var executor = Executors.newSingleThreadExecutor();
        var future = executor.submit(
                        () -> {
                            try {
                                command.execute("test_user",
                                        new MetricClientProvider.NoopMetricClientProvider(),
                                        eventConsumer);
                            } catch (Exception e) {
                                log.error(e.getMessage(), e);
                                Assertions.assertNull(e, e.getMessage());
                            }
                        });


        var n = 100;
        sendTestData(n);

        log.info("Trying to read records");
        waitAndFetchRecords(eventConsumer, n);
        var records = eventConsumer.receivedEvents;
        future.cancel(true);
        executor.shutdown();

        log.info("Got {} records", records.size());

        assertEquals(n, records.size());
    }

    @BeforeAll
    public static void setup() {
        LOCALSTACK.start();

        kinesisClient = KinesisClient.builder()
                .endpointOverride(LOCALSTACK.getEndpointOverride(KINESIS))
                .region(Region.of(LOCALSTACK.getRegion()))
                .credentialsProvider(credentialsProvider)
                .build();

        var response = kinesisClient.createStream(CreateStreamRequest.builder()
                .streamName(STREAM_NAME)
                .shardCount(1)
                .build());
        if(response.sdkHttpResponse().statusCode() != 200) {
            throw new RuntimeException("Failed to create stream: " + response.sdkHttpResponse().statusCode());
        }

        waitForStreamToBecomeActive();
    }

    private static void waitForStreamToBecomeActive() {
        var start = System.currentTimeMillis();

        while (true) {
            var status = kinesisClient.describeStream(
                            DescribeStreamRequest.builder().streamName(STREAM_NAME).build())
                    .streamDescription().streamStatusAsString();
            log.info("{}: {}", STREAM_NAME, status);
            if ("ACTIVE".equals(status)) break;
            try { Thread.sleep(500); } catch (InterruptedException ignored) {}
            log.info("Waiting for {} to become ACTIVE...", STREAM_NAME);
            if(System.currentTimeMillis() - start > 60000) {
                throw new RuntimeException("Stream " + STREAM_NAME + " is not available");
            }
        }
    }

    @SneakyThrows
    private static void waitAndFetchRecords(TestSourceEventAcceptor fetcher, int waitForN) {
        int maxAttempts = 60 * 10;
        int delayMillis = 10_000;

        for(int i = 0; i < maxAttempts; i++) {
            if(fetcher.receivedEvents.size() < waitForN) {
                Thread.sleep(delayMillis);
            }
        }
    }

    private static void sendTestData(int n) {
        for (int i = 0; i < n; i++) {
            var data = "record-" + i;
            var resp = kinesisClient.putRecord(PutRecordRequest.builder()
                    .streamName(STREAM_NAME)
                    .partitionKey(UUID.randomUUID().toString())
                    .data(SdkBytes.fromUtf8String(data))
                    .build());
            assertTrue(resp.sdkHttpResponse().isSuccessful());
        }
    }


    @AfterAll
    public static void teardown() {
        if (kinesisClient != null) kinesisClient.close();
        LOCALSTACK.stop();
    }

    public static class TestSourceEventAcceptor implements SourceEventAcceptor {
        private final List<RecordFleakData> receivedEvents =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public void terminate() {}

        @Override
        public void accept(List<RecordFleakData> recordFleakData) {
            receivedEvents.addAll(recordFleakData);
        }
    }
}
