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
package io.fleak.zephflow.api;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** A path relative to an operator configuration, before any presentation-specific escaping. */
public record ConfigurationPath(List<Segment> segments) implements Serializable {
  public ConfigurationPath {
    segments = List.copyOf(segments);
  }

  /** A field name and an array index are distinct, including for numeric map keys. */
  public sealed interface Segment extends Serializable permits Field, Index {}

  /** An object property or map key, including empty names and names containing punctuation. */
  public record Field(String name) implements Segment {
    public Field {
      Objects.requireNonNull(name, "name");
    }
  }

  /** A zero-based index into the configuration supplied by the caller. */
  public record Index(int value) implements Segment {
    public Index {
      if (value < 0) throw new IllegalArgumentException("Configuration index must be nonnegative");
    }
  }

  /** The configuration object itself. */
  public static ConfigurationPath root() {
    return new ConfigurationPath(List.of());
  }

  /** Constructs an object path without interpreting dots, brackets, or numeric property names. */
  public static ConfigurationPath of(String... fields) {
    return new ConfigurationPath(Arrays.stream(fields).<Segment>map(Field::new).toList());
  }

  /** Appends a literal property name. */
  public ConfigurationPath field(String name) {
    return append(new ConfigurationPath(List.of(new Field(name))));
  }

  /** Appends an input array index. */
  public ConfigurationPath index(int value) {
    return append(new ConfigurationPath(List.of(new Index(value))));
  }

  /** Appends the relative path returned by a delegated parser or validator. */
  public ConfigurationPath append(ConfigurationPath relative) {
    var combined = new ArrayList<>(segments);
    combined.addAll(relative.segments);
    return new ConfigurationPath(combined);
  }
}
