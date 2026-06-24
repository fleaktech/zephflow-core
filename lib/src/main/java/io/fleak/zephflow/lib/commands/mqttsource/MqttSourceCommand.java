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
package io.fleak.zephflow.lib.commands.mqttsource;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

@Slf4j
public class MqttSourceCommand extends SimpleSourceCommand<SerializedEvent> {

  private final MqttClientProvider mqttClientProvider;

  public MqttSourceCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      MqttClientProvider mqttClientProvider) {
    super(nodeId, jobContext, configParser, configValidator);
    this.mqttClientProvider = mqttClientProvider;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    MqttSourceDto.Config config = (MqttSourceDto.Config) commandConfig;

    Fetcher<SerializedEvent> fetcher = createMqttFetcher(config, jobContext);
    RawDataEncoder<SerializedEvent> encoder = new BytesRawDataEncoder();
    RawDataConverter<SerializedEvent> converter = createRawDataConverter(config);

    Map<String, String> metricTags =
        basicCommandMetricTags(jobContext.getMetricTags(), commandName(), nodeId);
    FleakCounter dataSizeCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_SIZE_COUNT, metricTags);
    FleakCounter inputEventCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_COUNT, metricTags);
    FleakCounter deserializeFailureCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_DESER_ERR_COUNT, metricTags);

    String keyPrefix = (String) jobContext.getOtherProperties().get(JobContext.DATA_KEY_PREFIX);
    DlqWriter dlqWriter =
        Optional.of(jobContext)
            .map(JobContext::getDlqConfig)
            .map(c -> DlqWriterFactory.createDlqWriter(c, keyPrefix))
            .orElse(null);
    if (dlqWriter != null) {
      dlqWriter.open();
    }

    return new SourceExecutionContext<>(
        fetcher,
        converter,
        encoder,
        dataSizeCounter,
        inputEventCounter,
        deserializeFailureCounter,
        dlqWriter);
  }

  private Fetcher<SerializedEvent> createMqttFetcher(
      MqttSourceDto.Config config, JobContext jobContext) {
    MqttClient mqttClient = null;
    try {
      mqttClient = mqttClientProvider.createClient(config);

      UsernamePasswordCredential credential = null;
      if (StringUtils.isNotBlank(config.getCredentialId())) {
        credential = lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
      }
      MqttConnectionOptions connectionOptions =
          mqttClientProvider.buildConnectionOptions(config, credential);

      BlockingQueue<byte[]> messageQueue =
          new LinkedBlockingQueue<>(config.getReceiveQueueCapacity());
      mqttClient.setCallback(new EnqueueingCallback(messageQueue));
      mqttClient.connect(connectionOptions);
      mqttClient.subscribe(config.getTopicFilter(), config.getQos());

      return new MqttSourceFetcher(
          mqttClient, messageQueue, config.getReceiveTimeoutMs(), config.getMaxBatchSize());
    } catch (Exception e) {
      MqttSourceFetcher.closeQuietly(mqttClient);
      throw new RuntimeException("Failed to create MQTT fetcher", e);
    }
  }

  private RawDataConverter<SerializedEvent> createRawDataConverter(MqttSourceDto.Config config) {
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(config.getEncodingType())
            .createDeserializer();
    return new BytesRawDataConverter(deserializer);
  }

  @Override
  public SourceType sourceType() {
    return SourceType.STREAMING;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_MQTT_SOURCE;
  }

  private record EnqueueingCallback(BlockingQueue<byte[]> messageQueue) implements MqttCallback {

    @Override
    public void messageArrived(String topic, MqttMessage message) {
      if (!messageQueue.offer(message.getPayload())) {
        log.warn("MQTT receive queue is full, dropping message from topic {}", topic);
      }
    }

    @Override
    public void disconnected(MqttDisconnectResponse disconnectResponse) {
      log.warn("MQTT client disconnected: {}", disconnectResponse.getReasonString());
    }

    @Override
    public void mqttErrorOccurred(MqttException exception) {
      log.error("MQTT error occurred", exception);
    }

    @Override
    public void deliveryComplete(org.eclipse.paho.mqttv5.client.IMqttToken token) {}

    @Override
    public void connectComplete(boolean reconnect, String serverUri) {}

    @Override
    public void authPacketArrived(int reasonCode, MqttProperties properties) {}
  }
}
