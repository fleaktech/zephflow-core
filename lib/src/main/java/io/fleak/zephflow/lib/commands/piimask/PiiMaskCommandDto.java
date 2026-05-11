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
package io.fleak.zephflow.lib.commands.piimask;

import io.fleak.zephflow.api.CommandConfig;
import java.util.List;

public interface PiiMaskCommandDto {

  record Config(List<String> targets, Detectors detectors, List<CustomPattern> customPatterns)
      implements CommandConfig {}

  record Detectors(
      DetectorConfig email,
      DetectorConfig phone,
      DetectorConfig ssn,
      DetectorConfig creditCard,
      DetectorConfig ipv4,
      DetectorConfig ipv6) {}

  // null replacement -> fall back to the built-in default token
  record DetectorConfig(String replacement) {}

  record CustomPattern(String name, String pattern, String replacement) {}
}
