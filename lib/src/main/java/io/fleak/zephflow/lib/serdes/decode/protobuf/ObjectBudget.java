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
package io.fleak.zephflow.lib.serdes.decode.protobuf;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.WireFormat;
import java.io.IOException;

/**
 * Counts what a message would build before protobuf parses it: every message instance costs its
 * fields plus itself (unset fields are filled in with defaults), every repeated item and map entry
 * one more, and a packed field at most one per byte. Parsing alone builds a tree of the same shape,
 * so the check has to come first.
 */
final class ObjectBudget {

  private static final int MAX_DEPTH = 100;

  private long left;

  private ObjectBudget(long left) {
    this.left = left;
  }

  static void check(Descriptor type, byte[] bytes, long budget) throws IOException {
    new ObjectBudget(budget).message(type, CodedInputStream.newInstance(bytes), 0, false);
  }

  private void message(Descriptor type, CodedInputStream in, int depth, boolean group)
      throws IOException {
    if (depth > MAX_DEPTH) {
      throw new MalformedProtobufException("message nests deeper than " + MAX_DEPTH + " levels");
    }
    charge(type.getFields().size() + 1L);
    while (true) {
      int tag = in.readTag();
      if (tag == 0 || (group && WireFormat.getTagWireType(tag) == WireFormat.WIRETYPE_END_GROUP)) {
        return;
      }
      FieldDescriptor field = type.findFieldByNumber(WireFormat.getTagFieldNumber(tag));
      int wireType = WireFormat.getTagWireType(tag);
      if (field == null) {
        in.skipField(tag);
        continue;
      }
      charge(field.isMapField() ? 2 : 1);
      if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
          && wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
        int length = in.readRawVarint32();
        int limit = in.pushLimit(length);
        message(field.getMessageType(), in, depth + 1, false);
        in.popLimit(limit);
      } else if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
          && wireType == WireFormat.WIRETYPE_START_GROUP) {
        message(field.getMessageType(), in, depth + 1, true);
      } else if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED && field.isPackable()) {
        int length = in.readRawVarint32();
        charge(length);
        in.skipRawBytes(length);
      } else {
        in.skipField(tag);
      }
    }
  }

  private void charge(long objects) throws MalformedProtobufException {
    left -= objects;
    if (left < 0) {
      throw new MalformedProtobufException(
          "message would build more than "
              + ProtobufRecords.OBJECTS_PER_BYTE
              + " values per byte plus "
              + ProtobufRecords.EXTRA_OBJECTS);
    }
  }
}
