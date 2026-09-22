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
package io.fleak.zephflow.lib.commands.fssource.checkpoint;

import io.fleak.zephflow.lib.utils.JsonUtils;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** HTTP-backed (or in-memory) checkpoint state store. */
public interface CheckpointClient extends AutoCloseable {
  Logger log = LoggerFactory.getLogger(CheckpointClient.class);

  void checkpoint(String id, String data);

  Optional<CheckpointData> loadCheckpoint(String id);

  /** No-op by default; implementations with closeable resources should override. */
  default void close() {}

  record CheckpointData(long ts, String data) {}

  class InMemCheckpointClient implements CheckpointClient {
    private final Map<String, LinkedList<CheckpointData>> store = new HashMap<>();

    @Override
    public synchronized void checkpoint(String id, String data) {
      store
          .computeIfAbsent(id, k -> new LinkedList<>())
          .add(new CheckpointData(System.currentTimeMillis(), data));
    }

    @Override
    public synchronized Optional<CheckpointData> loadCheckpoint(String id) {
      var checkpoints = store.get(id);
      if (CollectionUtils.isEmpty(checkpoints)) {
        return Optional.empty();
      }
      return Optional.of(checkpoints.getLast());
    }
  }

  class HttpCheckpointClient implements CheckpointClient {
    static final String VERSION_HEADER = "X-State-Version";
    static final String EXPECTED_VERSION_HEADER = "X-State-Expected-Version";

    private final HttpClient client = HttpClient.newHttpClient();
    private final String baseUrl;
    private final Map<String, Long> versionById = new ConcurrentHashMap<>();
    private final AtomicBoolean legacyServerWarned = new AtomicBoolean(false);
    private volatile boolean versionSupported = true;

    public HttpCheckpointClient(String baseUrl) {
      baseUrl = Objects.requireNonNull(baseUrl).trim();
      if (baseUrl.endsWith("/")) {
        baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
      }
      this.baseUrl = baseUrl;
    }

    @Override
    public synchronized void checkpoint(String id, String data) {
      try {
        String json = JsonUtils.toJsonString(new CheckpointData(System.currentTimeMillis(), data));
        HttpRequest.Builder requestBuilder =
            HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/" + id))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8));
        if (versionSupported) {
          requestBuilder.header(
              EXPECTED_VERSION_HEADER, String.valueOf(versionById.getOrDefault(id, 0L)));
        }
        HttpResponse<String> response =
            client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 409) {
          throw new CheckpointConflictException(id);
        }
        if (response.statusCode() < 200 || response.statusCode() > 299) {
          throw new CheckpointException("Failed to checkpoint state for id=" + id);
        }
        readVersion(response).ifPresent(version -> versionById.put(id, version));
      } catch (RuntimeException runtimeException) {
        throw runtimeException;
      } catch (Exception exception) {
        throw new CheckpointException("Failed to checkpoint state for id=" + id, exception);
      }
    }

    @Override
    public synchronized Optional<CheckpointData> loadCheckpoint(String id) {
      try {
        HttpRequest request =
            HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/" + id))
                .header("Accept", "application/json")
                .GET()
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() > 299) {
          throw new CheckpointException(
              "Failed to load checkpoint for id=" + id + "; response=" + response.body());
        }
        Optional<Long> version = readVersion(response);
        if (version.isEmpty()) {
          noteLegacyServer();
        } else {
          versionById.put(id, version.get());
        }
        String body = response.body();
        if (StringUtils.isBlank(body)) {
          return Optional.empty();
        }
        CheckpointData checkpoint = JsonUtils.fromJsonString(body, CheckpointData.class);
        if (checkpoint == null || checkpoint.data() == null) {
          return Optional.empty();
        }
        return Optional.of(checkpoint);
      } catch (RuntimeException runtimeException) {
        throw runtimeException;
      } catch (Exception exception) {
        throw new CheckpointException("Failed to load checkpoint for id=" + id, exception);
      }
    }

    /**
     * Absent means an older jobmaster that never reports a version: legacy fallback applies.
     * Present but unparseable means a current server violating its own contract, which must not be
     * treated the same way, or the fence silently disappears.
     */
    private static Optional<Long> readVersion(HttpResponse<String> response) {
      Optional<String> header = response.headers().firstValue(VERSION_HEADER);
      if (header.isEmpty()) {
        return Optional.empty();
      }
      String value = header.get().trim();
      try {
        return Optional.of(Long.parseLong(value));
      } catch (NumberFormatException numberFormatException) {
        throw new CheckpointException(
            "checkpoint server returned an unparseable " + VERSION_HEADER + " value: " + value,
            numberFormatException);
      }
    }

    /** An older jobmaster: writes stay unconditional, and two attempts can still clobber. */
    private void noteLegacyServer() {
      versionSupported = false;
      if (legacyServerWarned.compareAndSet(false, true)) {
        log.warn(
            "checkpoint server at {} does not report {}; falling back to unconditional writes, so"
                + " concurrent attempts of the same source are not fenced",
            baseUrl,
            VERSION_HEADER);
      }
    }

    @Override
    public void close() {
      client.close();
    }
  }

  class CheckpointException extends RuntimeException {
    public CheckpointException(String message) {
      super(message);
    }

    public CheckpointException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  class CheckpointConflictException extends CheckpointException {
    public CheckpointConflictException(String id) {
      super(
          "checkpoint for id="
              + id
              + " was advanced by another writer; this attempt no longer owns the source");
    }
  }
}
