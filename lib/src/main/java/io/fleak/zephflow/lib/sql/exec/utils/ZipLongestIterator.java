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
package io.fleak.zephflow.lib.sql.exec.utils;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ZipLongestIterator<T, R> implements Iterator<R> {

  final List<Iterator<T>> its;
  final Function<List<T>, R> combineFn;
  private final T fillValue;
  private final Supplier<List<T>> constantPrefix;

  public ZipLongestIterator(
      Supplier<List<T>> constantPrefix,
      List<Iterator<T>> its,
      Function<List<T>, R> combineFn,
      T fillValue) {
    this.constantPrefix = constantPrefix;
    this.its = its;
    this.combineFn = combineFn;
    this.fillValue = fillValue;
  }

  @Override
  public boolean hasNext() {
    for (var it : its) {
      if (it.hasNext()) return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  @Override
  public R next() {
    int nulls = 0;
    var buff = new ArrayList<T>(its.size());
    buff.addAll(constantPrefix.get());

    for (var it : its) {
      if (it.hasNext()) {
        buff.add(it.next());
      } else {
        if (it instanceof NullProvider<?> np) buff.add((T) np.getNullValue());
        else buff.add(fillValue);
        nulls++;
      }
    }

    if (nulls == its.size()) throw new NoSuchElementException();

    return combineFn.apply(buff);
  }

  @SafeVarargs
  public static <T, R> ZipLongestIterator<T, R> of(
      Supplier<List<T>> constantPrefix,
      Function<List<T>, R> combineFn,
      T fillValue,
      Iterable<T>... ts) {
    return new ZipLongestIterator<>(
        constantPrefix,
        Arrays.stream(ts).map(Iterable::iterator).collect(Collectors.toList()),
        combineFn,
        fillValue);
  }
}
