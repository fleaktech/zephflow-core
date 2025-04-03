package io.fleak.zephflow.api.structure;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import org.junit.jupiter.api.Test;

/** Created by bolei on 4/2/25 */
class RecordFleakDataTest {

  @Test
  void unwrap() {
    RecordFleakData recordFleakData = new RecordFleakData(new HashMap<>());
    recordFleakData.payload.put("k1", null);
    recordFleakData.payload.put(null, null);
    recordFleakData.payload.put("k3", new StringPrimitiveFleakData("v3"));

    var actual = recordFleakData.unwrap();
    var expected = new HashMap<String, Object>();
    expected.put("k1", null);
    expected.put(null, null);
    expected.put("k3", "v3");
    assertEquals(expected, actual);
  }
}
