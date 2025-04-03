package io.fleak.zephflow.api.structure;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import org.junit.jupiter.api.Test;

/** Created by bolei on 4/2/25 */
class ArrayFleakDataTest {

  @Test
  void unwrap() {
    ArrayFleakData arrayFleakData = new ArrayFleakData(new ArrayList<>());
    arrayFleakData.arrayPayload.add(FleakData.wrap(5));
    arrayFleakData.arrayPayload.add(null);

    ArrayList<Object> expected = new ArrayList<>();
    expected.add(5);
    expected.add(null);
    assertEquals(expected, arrayFleakData.unwrap());
  }
}
