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
        new Spliterators.AbstractSpliterator<List<T>>(Long.MAX_VALUE, 0) {
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
