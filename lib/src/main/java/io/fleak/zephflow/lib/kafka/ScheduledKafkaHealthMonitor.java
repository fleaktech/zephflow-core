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
package io.fleak.zephflow.lib.kafka;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;

/** Monitors and outputs the health of a kafka connection */
@Slf4j
public class ScheduledKafkaHealthMonitor implements Runnable, KafkaHealthMonitor {
  private final long checkIntervalMillis;
  private final HealthListener healthListener;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private ScheduledExecutorService executor;
  private final AdminClient adminClient;

  public ScheduledKafkaHealthMonitor(
      AdminClient adminClient, long checkIntervalMillis, HealthListener healthListener) {
    this.adminClient = adminClient;
    this.checkIntervalMillis = checkIntervalMillis;
    this.healthListener = healthListener;
  }

  public void start() {
    if (!running.get()) {
      synchronized (this) {
        if (!running.get()) {
          executor =
              Executors.newScheduledThreadPool(
                  1,
                  r -> {
                    var thread = new Thread(r);
                    // must be a daemon so it doesn't block shutdown
                    thread.setDaemon(true);
                    return thread;
                  });
          executor.scheduleWithFixedDelay(this, 1000, checkIntervalMillis, TimeUnit.MILLISECONDS);
          running.set(true);
        }
      }
    }
  }

  public void stop() {
    if (running.get()) {
      synchronized (this) {
        if (running.get()) {
          running.set(false);
          executor.shutdown();
        }
      }
    }
  }

  @Override
  public void run() {
    log.debug("Kafka health monitor check running: {} ", running.get());
    try {
      if (!running.get()) return;

      var cluster = adminClient.describeCluster();
      var nodes = cluster.nodes().get(checkIntervalMillis, TimeUnit.MILLISECONDS);
      log.debug("nodes: {}", nodes);

      if (nodes.isEmpty()) {
        throw new IllegalStateException("No kafka broker nodes available");
      }
    } catch (Exception e) {
      log.debug("error {}", e.getMessage());
      if (running.get()) healthListener.cannotConnect(e);
    } finally {
      if (!running.get()) {
        closeAdminClient();
      }
    }
  }

  private void closeAdminClient() {
    try {
      adminClient.close();
    } catch (Exception e) {
      log.error("failed to close admin client", e);
    }
  }

  public interface HealthListener {
    void cannotConnect(Exception exception);
  }
}
