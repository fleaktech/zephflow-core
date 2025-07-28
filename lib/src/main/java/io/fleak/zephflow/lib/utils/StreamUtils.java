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
package io.fleak.zephflow.lib.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StreamUtils {
  public static <T> Stream<List<T>> partition(Stream<T> stream, int size) {
    Iterator<T> iterator = stream.iterator();
    return StreamSupport.stream(
        new Spliterators.AbstractSpliterator<>(Long.MAX_VALUE, 0) {
          @Override
          public boolean tryAdvance(Consumer<? super List<T>> action) {
            List<T> batch = new ArrayList<>(size);
            while (iterator.hasNext() && batch.size() < size) {
              batch.add(iterator.next());
            }
            if (batch.isEmpty()) return false;
            action.accept(batch);
            return true;
          }
        },
        false);
  }
}
