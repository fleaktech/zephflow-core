package io.fleak.zephflow.lib.commands.kinesissource;

import io.fleak.zephflow.lib.serdes.SerializedEvent;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
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

import java.util.ArrayList;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.KINESIS;

@Slf4j
public class KinesisSourceFetcherTest {

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
            String status = kinesisClient.describeStream(
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

    @Test
    public void test() throws Exception {
        var config = KinesisSourceDto.Config.builder()
                .encodingType("TEXT")
                .streamName(STREAM_NAME)
                .regionStr(LOCALSTACK.getRegion())
                .applicationName(APPLICATION_NAME)
                .cloudWatchEndpoint(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.CLOUDWATCH).toString())
                .kinesisEndpoint(LOCALSTACK.getEndpointOverride(KINESIS).toString())
                .dynamoEndpoint(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString())
                .staticCredentials(new KinesisSourceDto.StaticCredentials(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()))
                .initialPosition(InitialPositionInStream.TRIM_HORIZON.name())
                .disableMetrics(true)
                .build();

        var fetcher = new KinesisSourceFetcher(config);

        int n = 100;
        sendTestData(n);

        log.info("Trying to read records");
        var records = new ArrayList<SerializedEvent>();
        waitAndFetchRecords(fetcher, records);
        fetcher.close();
        log.info("Got {} records", records.size());

        assertEquals(n, records.size());

        var recordsSet = records.stream()
                .map(r -> new String(r.value()))
                .collect(Collectors.toSet());

        assertEquals(n, recordsSet.size());
    }

    private static void waitAndFetchRecords(KinesisSourceFetcher fetcher, ArrayList<SerializedEvent> records) throws Exception {
        int maxAttempts = 60 * 10;
        int delayMillis = 10_000;

        for(int i = 0; i < maxAttempts; i++) {
            var list = fetcher.fetch();
            if(list.isEmpty()) {
                if(!records.isEmpty())
                    break;
                Thread.sleep(delayMillis);
                System.out.println("Fetched events: " + list.size());
            } else {
                records.addAll(list);
                fetcher.commiter().commit();
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

}
