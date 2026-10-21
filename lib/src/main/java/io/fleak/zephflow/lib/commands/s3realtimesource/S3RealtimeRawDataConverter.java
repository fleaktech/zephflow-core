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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.commands.filedecode.DlqTexts;
import io.fleak.zephflow.lib.commands.filedecode.RecordRanges;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun.Counters;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun.Policy;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun.Settings;
import io.fleak.zephflow.lib.commands.s3realtimesource.S3ObjectDecoder.Result;
import io.fleak.zephflow.lib.commands.source.BatchEmittingConverter;
import io.fleak.zephflow.lib.commands.source.RawDataEncoder;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import java.nio.file.Path;
import java.util.Map;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Decodes the S3 objects an {@link S3EventMessage} names, one at a time, and emits each object's
 * records in bounded batches while it is decoded ({@link UnitRun}). A bad record that carries at
 * most 128 KiB is dead-lettered with its own bytes; every other failure is summarized once per
 * object and kind, and downstream failures once per object and failing node, with the notification
 * as the value. The message's receipt handle is queued for deletion only when it is acknowledged.
 */
@Slf4j
public class S3RealtimeRawDataConverter implements BatchEmittingConverter<S3EventMessage> {

  static final String S3_METADATA_BUCKET = "__s3_bucket";
  static final String S3_METADATA_KEY = "__s3_key";
  static final int MAX_RECORD_ENTRY_BYTES = UnitRun.MAX_RECORD_ENTRY_BYTES;
  private static final String RETRY = "then re-upload the object or send this notification again";

  private final S3RegionalClientProvider s3Clients;
  private final S3ObjectDecoder decoder;
  private final long maxObjectSizeBytes;
  private final Path spoolDir;
  private final Queue<String> confirmedReceiptHandles;
  private final RawDataEncoder<S3EventMessage> encoder;
  private final FleakCounter skippedObjectCounter;
  private final Settings settings;

  public S3RealtimeRawDataConverter(
      S3RegionalClientProvider s3Clients,
      S3ObjectDecoder decoder,
      String format,
      long maxObjectSizeBytes,
      Path spoolDir,
      long batchBytes,
      Queue<String> confirmedReceiptHandles,
      DlqWriter dlqWriter,
      RawDataEncoder<S3EventMessage> encoder,
      String nodeId,
      FleakCounter skippedObjectCounter) {
    this.s3Clients = s3Clients;
    this.decoder = decoder;
    this.maxObjectSizeBytes = maxObjectSizeBytes;
    this.spoolDir = spoolDir;
    this.confirmedReceiptHandles = confirmedReceiptHandles;
    this.encoder = encoder;
    this.skippedObjectCounter = skippedObjectCounter;
    this.settings =
        new Settings(
            new DlqTexts(
                format, decoder.multiline(), decoder.heapBytes(), "object", RETRY, null, false),
            Policy.SUMMARIZE,
            batchBytes,
            dlqWriter,
            nodeId);
  }

  @Override
  public void emit(S3EventMessage message, SourceExecutionContext<?> context, Emitter emitter) {
    for (S3ObjectRef ref : message.objectRefs()) {
      if (emitter.stopped()) {
        return;
      }
      new ObjectRun(message, ref, context, emitter).run();
    }
  }

  @Override
  public void acknowledge(S3EventMessage message) {
    confirmedReceiptHandles.add(message.receiptHandle());
  }

  static long estimate(FleakData data) {
    return UnitRun.estimate(data);
  }

  static String causeName(Throwable error) {
    return UnitRun.causeName(error);
  }

  static final class IndexRanges extends RecordRanges {}

  /** Decoding one object, with the notification as the value of its summaries. */
  private final class ObjectRun implements UnitRun.Unit {
    private final S3EventMessage message;
    private final S3ObjectRef ref;
    private final UnitRun run;
    private String versionId;

    ObjectRun(
        S3EventMessage message,
        S3ObjectRef ref,
        SourceExecutionContext<?> context,
        Emitter emitter) {
      this.message = message;
      this.ref = ref;
      this.run =
          new UnitRun(
              settings,
              new Counters(
                  context.inputEventCounter(),
                  context.dataSizeCounter(),
                  context.deserializeFailureCounter()),
              this,
              emitter);
    }

    void run() {
      Result result;
      try {
        S3Client client = s3Clients.clientFor(ref.region());
        result = decoder.decode(client, ref, maxObjectSizeBytes, spoolDir, run::visit);
      } catch (Exception e) {
        run.finishWithFailure(
            "READ_FAILURE",
            "READ_FAILURE s3 object: " + causeName(e),
            "check the cause below, such as the source's S3 permissions (s3:GetObject), the"
                + " network, or free disk where the task runs; "
                + RETRY,
            e);
        return;
      }
      versionId = result.versionId();
      switch (result.status()) {
        case MISSING -> {
          skippedObjectCounter.increase(Map.of("reason", "missing"));
          log.warn(
              "S3 object s3://{}/{} no longer exists; skipping the notification",
              ref.bucket(),
              ref.key());
        }
        case TOO_LARGE -> {
          skippedObjectCounter.increase(Map.of("reason", "oversized"));
          run.writeEntry(
              "OBJECT_TOO_LARGE s3 object\nAction: raise maxObjectSizeBytes above the object's"
                  + " size, with that much free disk for the task and a visibilityTimeoutSeconds"
                  + " long enough to download it (lower maxNumberOfMessages if several arrive"
                  + " together); "
                  + RETRY
                  + "\n"
                  + name()
                  + " size "
                  + result.storedLength()
                  + " exceeds maxObjectSizeBytes "
                  + maxObjectSizeBytes);
        }
        case STOPPED -> {}
        case FINISHED -> run.complete(result.storedLength());
      }
    }

    @Override
    public String name() {
      return "object " + location() + (versionId == null ? "" : " version " + versionId);
    }

    @Override
    public String location() {
      return "s3://" + ref.bucket() + "/" + ref.key();
    }

    @Override
    public SerializedEvent recordValue(DecodeOutcome.Failed failed) {
      return new SerializedEvent(encoder.serialize(message).key(), failed.raw(), null);
    }

    @Override
    public SerializedEvent summaryValue() {
      return encoder.serialize(message);
    }
  }
}
