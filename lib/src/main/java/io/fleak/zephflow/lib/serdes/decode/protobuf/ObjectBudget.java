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
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Counts what a message would build before protobuf parses it: every message instance costs itself
 * plus the fields that appear even when unset (repeated, map and scalars without presence), every
 * field occurrence one more (unknown ones too, which protobuf keeps), a map entry two (and its
 * value's defaults when the value is missing), and a packed field at most one per byte. Parsing
 * alone builds a tree of the same shape, so the check has to come first.
 */
final class ObjectBudget {

  static final long OBJECTS_PER_BYTE = 4;
  static final long EXTRA_OBJECTS = 100_000;

  private static final int MAX_DEPTH = 100;

  private final Map<Descriptor, Long> alwaysBuilt = new IdentityHashMap<>();
  private long left;

  private ObjectBudget(long left) {
    this.left = left;
  }

  static void check(Descriptor type, byte[] bytes) throws IOException {
    new ObjectBudget(OBJECTS_PER_BYTE * bytes.length + EXTRA_OBJECTS)
        .message(type, CodedInputStream.newInstance(bytes), 0, false);
  }

  /** Returns whether field 2 occurred, which for a map entry is its value. */
  private boolean message(Descriptor type, CodedInputStream in, int depth, boolean group)
      throws IOException {
    if (depth > MAX_DEPTH) {
      throw new MalformedProtobufException("message nests deeper than " + MAX_DEPTH + " levels");
    }
    charge(1 + alwaysBuilt(type));
    boolean secondField = false;
    while (true) {
      int tag = in.readTag();
      int wireType = WireFormat.getTagWireType(tag);
      if (tag == 0 || (group && wireType == WireFormat.WIRETYPE_END_GROUP)) {
        return secondField;
      }
      FieldDescriptor field = type.findFieldByNumber(WireFormat.getTagFieldNumber(tag));
      FieldDescriptor.Type fieldType = field == null ? null : field.getType();
      if (fieldType == FieldDescriptor.Type.MESSAGE
          && wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
        charge(field.isMapField() ? 2 : 1);
        int limit = in.pushLimit(in.readRawVarint32());
        boolean hasValue = message(field.getMessageType(), in, depth + 1, false);
        in.popLimit(limit);
        if (field.isMapField() && !hasValue) {
          defaultValue(field.getMessageType().findFieldByNumber(2));
        }
        secondField |= field.getNumber() == 2;
      } else if (fieldType == FieldDescriptor.Type.GROUP
          && wireType == WireFormat.WIRETYPE_START_GROUP) {
        charge(1);
        message(field.getMessageType(), in, depth + 1, true);
        secondField |= field.getNumber() == 2;
      } else if (field != null
          && field.isPackable()
          && wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
        int length = in.readRawVarint32();
        charge(1L + length);
        in.skipRawBytes(length);
      } else {
        other(in, tag, depth);
      }
    }
  }

  /** A scalar, or a field protobuf keeps as unknown, where a group becomes a tree of its own. */
  private void other(CodedInputStream in, int tag, int depth) throws IOException {
    charge(1);
    if (WireFormat.getTagWireType(tag) != WireFormat.WIRETYPE_START_GROUP) {
      in.skipField(tag);
      return;
    }
    if (depth + 1 > MAX_DEPTH) {
      throw new MalformedProtobufException("message nests deeper than " + MAX_DEPTH + " levels");
    }
    while (true) {
      int inner = in.readTag();
      if (inner == 0 || WireFormat.getTagWireType(inner) == WireFormat.WIRETYPE_END_GROUP) {
        return;
      }
      other(in, inner, depth + 1);
    }
  }

  /** A map entry without its value still converts to one, a record of defaults for a message. */
  private void defaultValue(FieldDescriptor value) throws MalformedProtobufException {
    if (value.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
      charge(1 + alwaysBuilt(value.getMessageType()));
    }
  }

  private long alwaysBuilt(Descriptor type) {
    return alwaysBuilt.computeIfAbsent(type, ObjectBudget::fieldsWithoutPresence);
  }

  private static long fieldsWithoutPresence(Descriptor type) {
    return type.getFields().stream().filter(f -> f.isRepeated() || !f.hasPresence()).count();
  }

  private void charge(long objects) throws MalformedProtobufException {
    left -= objects;
    if (left < 0) {
      throw new MalformedProtobufException(
          "message would build more than "
              + OBJECTS_PER_BYTE
              + " values per byte plus "
              + EXTRA_OBJECTS);
    }
  }
}
