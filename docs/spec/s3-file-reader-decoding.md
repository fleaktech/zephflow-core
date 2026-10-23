# S3 File Reader on the Generic Decoder

The `s3filereader` node (`S3FileReaderCommand`) reads, for each input record, the object named by
an `s3://bucket/key` path in the record's `pathField`, and emits the object's lines as records. With
`emission: DESERIALIZE` it decodes each line on its own with the legacy `FleakDeserializer` of
`encodingType`. This change makes it decode each line with the generic decoder
(`docs/spec/generic-decoder.md`). It keeps the configuration, the failures outside line decoding,
and the records of ordinary lines; the lines of a UTF-8 object that give a different result are
all listed in 4.1.

## 1. Why

The S3 file reader is one of the last users of the legacy deserializers in this repository. Moving it
onto the generic decoder is a step toward removing them. It gains no new formats or options.

## 2. Goals and non-goals

**Goals**
- `emission: DESERIALIZE` decodes each line through the generic decoder.
- The node's own code no longer uses `serdes.des` or the legacy decompressors.
- A line that fails to decode fails the input record with a clear, actionable error message.
- Every difference in records or failures for UTF-8 objects is listed, with its reason (4.1).

**Non-goals**
- A `decoding` field, new formats or compressions, whole-object decoding.
- Adapters that copy the legacy deserializers' handling of unusual lines (4.1 lists them instead).
- Objects too large for memory: the node keeps reading a whole object into a list, as today.
- `emission: LINE`, the S3 client, credentials, path parsing, `urlDecodeKey`, `compression`, the
  validator and the counters: unchanged.

## 3. Design

### 3.1 Per line

At initialization, with `emission: DESERIALIZE`:

- For PARQUET and KV the node throws the same `UnsupportedOperationException` and message as
  today ("PARQUET is a sink-only format (not supported for deserialization)", and the same for KV),
  so these configs still pass validation and fail when the node starts.
- For the other seven `encodingType`s it compiles a `RecordDecoder` from
  `MessageDecodingConverter.decodingConfig(encodingType, null, false)`: the format of
  `SourceDecoding`'s mapping, with no compression.

Gunzipping stays where it is, on the whole object stream, chosen by `compression` (`AUTO` by the
`.gz` key suffix, `NONE`, `GZIP`), with the JDK's `GZIPInputStream`. The object is read line by line
as today (`InputStreamReader` with UTF-8, `BufferedReader.readLine`), and lines are numbered from 1,
blank lines included. For each line:

1. With TEXT, an empty line gives the record `{"__raw__": ""}`, as today. (The generic decoder gives
   no record for an empty input; this keeps today's record. It is the only special case.)
2. Otherwise the line's UTF-8 bytes are decoded with `decodeAll(bytes, DecodeLimits.UNLIMITED)`.
3. If every outcome is a decoded record, the records are appended to the output, in order.
4. If any outcome is not a decoded record, or decoding throws, the node logs the message of 3.2 with
   the decoder's error and its stack trace at WARN, then throws an `IllegalArgumentException` with
   that message and the decoder's error as its cause. As today, the input record fails as a whole
   and none of the object's records are emitted.

A line gets no metadata, as today (the legacy path built `SerializedEvent(null, bytes, null)`).

The node's code imports nothing from `serdes.des`. Its one remaining tie is shared with every other
source: `SourceDecoding` checks `encodingType` with `DeserializerFactory.validateEncodingType`. That
check is replaced when the legacy deserializers are removed.

### 3.2 Error message

`ScalarCommand` turns the exception's message into the input record's `ErrorOutput`, which later
error handling and the DLQ wrap again with their own stack trace, and it logs the failure only at
DEBUG. So the message holds no stack trace, and the node logs the decoder's error with its trace
itself (3.1 step 4). For a line that fails to decode the message is:

```
MALFORMED json line: JsonParseException
Action: check that each line of the object is what the node's encodingType JSON_OBJECT_LINE
  expects, or fix the object; the input record failed and no record of this object was emitted
Unrecognized token 'not': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')
s3://bucket/key line 3, recordIndex 1, bytes 0-8
```

(the third line of the object is `not json`; the Action is one line in the message, wrapped here).

- **First line:** `<kind> <format> line: <exception class>`. `<kind>` is the outcome's kind
  (`MALFORMED`); for an `Oversized` outcome or an exception thrown by the decoder it is
  `READ_FAILURE`. `<format>` and `<exception class>` are as in the message sources
  (`SourceDecoding.formatName`, `UnitRun.causeName`).
- **Action:** as above, with the node's `encodingType` value, for every kind.
- **Cause:** the first line of the innermost cause's message.
- **Location:** the path as it appears in the input record and `line N`, then `, ` and what the
  decoder knows, exactly as the message sources write it: `recordIndex N` when it is 1 or more,
  `bytes a-b` when the span's length is 0 or more, and `(and k more problems)` when the line had
  others. When the decoder knows none of these (every CSV failure, a decoder that throws), the
  location ends at `line N`. The node reuses `MessageDecodingConverter`'s private `location`
  method, made public static; nothing else in that class changes.

Other failures (a missing path, a bad URI, S3 errors, a broken gzip stream) keep today's messages.

## 4. Configuration

No field is added, removed or changed. `encodingType` keeps its default, JSON_OBJECT_LINE. The
validator is unchanged.

### 4.1 Behavior changes for existing configurations

1. **Error message** of a line that fails to decode: the text of 3.2 instead of the legacy
   deserializer's exception message. Today's JSON_OBJECT_LINE message quoted the line
   (`failed to parse json object line: <line>`); the new one names its position instead.
2. **Records and failures per line.** Each line is decoded by the generic decoder with the format of
   `encodingType`, the same format the message sources use, and no adapter copies the legacy
   deserializers' handling of other lines. So the lines below give a different result than today.
   Each row was measured against both paths and is pinned by the differential test (6).

   | encodingType | Line | Today | Now | Why |
   |---|---|---|---|---|
   | JSON_OBJECT_LINE | an array of objects, `[{"a":1},{"a":2}]` | one record per element | fails | The generic JSON lines format takes one object per line; arrays were a legacy extension. |
   | JSON_OBJECT_LINE | an empty array, `[]`, with or without whitespace inside or around it | no records | fails | Same as above. |
   | JSON_OBJECT_LINE | only characters that Java's `String.isBlank` treats as blank but JSON does not allow: vertical tab, form feed, U+001C to U+001F, Unicode spaces such as U+2028 or U+3000 | no records | fails | The legacy deserializer skipped every `isBlank` line; the generic decoder skips only JSON whitespace (space, tab, CR, LF). |
   | JSON_OBJECT_LINE | a line starting with a byte-order mark (U+FEFF) | fails | the mark is removed; the rest decodes, and a mark alone gives no records | The generic decoder removes a leading byte-order mark from every input, and here each line is an input. |
   | STRING_LINE, TEXT | a line starting with a byte-order mark | the mark kept in `__raw__` | the mark removed; with STRING_LINE a mark alone gives no records, with TEXT `{"__raw__": ""}` | Same as above. |
   | JSON_OBJECT, JSON_ARRAY, XML | an empty line | fails | no records | The generic decoder gives no record for an empty input; the legacy parsers failed on it. |
   | JSON_OBJECT_LINE, JSON_OBJECT, JSON_ARRAY | a string longer than 20,000,000 characters or a field name longer than 50,000 characters | fails | decodes | The legacy deserializers kept Jackson's default size limits; the generic decoder lifts them. |
   | XML | an element with more than 1,021 attributes | fails | decodes | The legacy XML parser kept Woodstox's default attribute limit; the generic decoder lifts it. |
   | XML | text after the root element, a second root element, or a `DOCTYPE` | one record | fails | The generic XML format reads exactly one document and refuses DTDs. |
   | CSV | a row with two cells that are equal once surrounding spaces and tabs and a leading byte-order mark are removed, e.g. `1,1` or ` a,a ` | no records | fails (duplicate header name) | Each line is its own CSV document, so its only row is a header, and the generic CSV format refuses duplicate header names. A file whose lines repeat a value now fails instead of giving no records. |
   | CSV | a row with a cell that is empty or holds only whitespace or control characters, e.g. `a,,b`, `a, ,b`, `,a`, `a,b,` or `""` | fails | no records | The legacy CSV parser trimmed whitespace and control characters from a cell and refused an empty header name; the generic one trims only spaces and tabs and accepts an empty header name. |
   | CSV | only whitespace or control characters, e.g. space, tab, vertical tab, form feed, NUL or U+2028 | fails | no records | The legacy CSV parser trimmed the cell to an empty header name and refused it; the generic one reads an empty header. |
   | CSV | a row with more than 16,384 cells | no records | fails | The generic CSV format limits a row to 16,384 columns. |

   Every line of a CSV object gives no records today, as now (each line is a header only), so the
   CSV rows change only whether the input record fails.

   The node's UTF-8 reader keeps a byte-order mark as U+FEFF wherever it is, so the rows on the mark
   apply to any line, not only the first. That reader replaces invalid UTF-8 with U+FFFD, as
   today, so such bytes reach both paths alike.

   The node reads objects as UTF-8 only, as today. An object in another encoding, such as UTF-16
   or UTF-32, is already garbled when the reader splits it into lines: most of its lines fail on
   both paths, and the result of some lines differs between today and now. Those per-line results
   are not listed here and are not pinned by the tests.

## 5. Compatibility

- **Config and validator:** unchanged.
- **Classes:** `S3FileReaderExecutionContext` holds a `RecordDecoder` instead of a
  `FleakDeserializer`, so its constructor changes. `MessageDecodingConverter.location` becomes
  public static. The command, DTO and validator keep their signatures. No class is added.
- **Output records:** unchanged except 4.1 item 2. **Error outputs:** unchanged except 4.1 items 1
  and 2.

## 6. Testing

The tests drive the node itself: `ScalarCommand.process` with `S3Backend.client` mocked (static
mock) to return an `S3Client` mock that serves the object's bytes, plain or gzipped.

- **Differential:** for each of the seven `encodingType`s, the same objects through today's
  per-line legacy path (the `DeserializerFactory` deserializer on each `readLine` result, as the
  legacy node did) and through the node, the records compared whole as their `unwrap()` maps
  (`RecordFleakData.equals` compares numbers with a tolerance and ignores their type). Cases:
  several lines, blank lines, a byte-order mark on the first and on a later line, and every row
  of 4.1 item 2, each pinned as an expected difference.
- **Error message:** a bad line after a blank line (`line 3`) gives an `ErrorOutput` whose message is
  compared whole, and so does a failing CSV line (a location that ends at `line N`); the error
  counter goes up by one, no record is emitted, and the WARN log line
  carries the decoder's error. A decoder that throws gives the `READ_FAILURE` first line.
- **Compression:** DESERIALIZE over a `.gz` key with `AUTO`, over a key without the suffix with
  `GZIP`, and a `.gz` key with `NONE` (not gunzipped).
- **LINE:** whole `{"line", "file"}` records compared for a small object.
- **PARQUET and KV:** the node fails to start with today's exception and message.
- **Existing tests:** the unit tests, the `MessageDecodingConverter` tests and the LocalStack test
  (`S3FileReaderCommandIntegrationTest`) stay green, and `./gradlew :lib:goldenTest` stays green
  (the node has no golden fixture).

## 7. Delivery

On the integration branch `feature/s3filereader-decoder`: PR 0 this spec, PR 1 the change and its
tests. The branch merges to main after PR 1, with approval.

## 8. Known limitations

- The node still reads a whole object into memory before it returns its records.
- One bad line fails the whole input record, and its good records are not emitted.
- CSV and TEXT stay per line, as today: each line is its own document, so a CSV object gives no
  records (each line is its own header) unless a line fails, and TEXT gives one record per line.
- Decoding line by line through `decodeAll` costs more than the legacy deserializer: for the default
  JSON_OBJECT_LINE, about 68 KB allocated per line against about 3 KB, and about 2.9 times the time
  (measured on 75-byte lines), because the decoder sets up a 64 KiB line buffer for each input. The
  node is meant for small objects; an object of 100,000 lines allocates several GB.
