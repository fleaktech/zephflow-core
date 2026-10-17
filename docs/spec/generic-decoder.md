# Generic Decoder

A configurable library in `lib` (`io.fleak.zephflow.lib.serdes.decode`) that turns bytes into
records. Deserialization only. Source commands are not wired to it yet; each source moves over in
its own later change.

## 1. Why

The current `lib/serdes` package does not cover what sources now receive:

- **Few formats, no parameters.** `EncodingType` is a fixed enum. CSV is always comma-separated with
  a header, and the charset cannot be set. There is no avro, protobuf or parquet.
- **Everything is held in memory.** `deserialize(SerializedEvent)` needs the whole payload. Only
  FsSource streams, and it does its own chunking.
- **No line-breaking.** Merging several lines into one record (stack traces, Windows events) is not
  possible, and line splitting is hard-wired into a few format classes.
- **Only gzip**, implemented several times in different places.
- **Many entry points and duplicate code**: four entry points on `FleakDeserializer`, three CSV
  parsers, and a factory, deserializer and converter for every format.

The generic decoder replaces this with one interface and one configuration that covers
compression, format and framing, and keeps memory bounded however large the input is.

## 2. Goals and non-goals

**Goals**
- One interface: a byte stream in, decode outcomes out, one at a time. A one-call helper for small
  inputs.
- One declarative configuration for compression, format and framing. Bad configuration fails when
  it is compiled, not while decoding.
- Bounded memory. At most one record, or one unit that must be held whole (a whole document, an
  avro block, a parquet row group), is in memory at a time. Going over a limit is reported, never an
  OOM.
- Every outcome is either a decoded record, a malformed record (with its raw bytes where possible),
  an oversized record, or a failure to read the input. Bad data never stops the caller from getting
  the good records before it.

**Non-goals**
- Serialization.
- Integrating sources, and compatibility with legacy `encodingType` configurations. Both come later,
  through an adapter outside the decoder; the decoder does not copy legacy quirks.
- Parsing text content. Text is only framed into records (`{"__raw__": "..."}`); syslog, CEF,
  key-value, Windows-event and grok parsing stay in the downstream `parser` node.
- Confluent wire format, Schema Registry, zip/tar archives, structured stack-trace parsing.
- Type inference: CSV, XML and text values are strings.
- Merging metadata (such as a Kafka key) into records; the caller does that.

## 3. Design

### 3.1 Pipeline

```
ByteInput (caller's stream; random access for parquet)
  → decompression chain (configured codecs, or "auto" = gzip detection)
  → transcoding to UTF-8 (text formats only, when the input is not UTF-8)
  → format: either a format with its own record boundaries (json, csv, xml, yaml, avro, protobuf,
            parquet), or text framing (line, multiline, whole)
  → caller pulls one DecodeOutcome at a time
```

The caller pulls. It batches as it likes, can stop at any point and close, and never has to chunk
the input itself, so multiline records are never cut at chunk boundaries.

### 3.2 API

```java
RecordDecoder decoder = DecodingConfigCompiler.compile(config);   // immutable, thread-safe, reusable
try (RecordCursor cursor = decoder.open(input, limits)) {
  DecodeOutcome outcome;
  while ((outcome = cursor.next()) != null) { ... }               // throws SourceReadException
}
DecodeResult all = decoder.decodeAll(bytes, limits);              // small inputs
```

- `ByteInput` supplies the raw (still compressed) stream. An input that knows its length must throw
  on a premature end rather than return EOF, because formats without an end marker cannot detect
  truncation themselves.
- `DecodeLimits(maxRecordBytes, maxDocumentBytes)` is chosen by the caller; 0 means unlimited.
- `DecodeOutcome` is one of:
  - `Decoded(record, recordIndex, span)`
  - `Failed(kind, raw, recordIndex, span, error, terminal)`, where `kind` is `MALFORMED` or
    `CORRUPT_COMPRESSION`
  - `Oversized(recordIndex, span, sizeSoFar, preview, terminal)`
- A failure to read the input itself (network, disk) throws `SourceReadException`. It is never
  reported as bad data.

### 3.3 Error model

- **Classification by origin.** The read, decompression and budget layers each mark a flag when
  they fail. When a format fails, ends, or gives up, one shared shell checks the flags in the order
  read → decompression → budget, and only then falls back to `MALFORMED`. The type of the exception
  that reaches the shell is never used, because parsers wrap and swallow errors.
- **Nothing decoded before a failure is lost.** Records produced from bytes delivered before the
  failure are returned first. (Some compression codecs can lose up to 255 bytes just before damaged
  data; see 4.1.)
- **`terminal`.** `false` means the next record can still be found and decoding continues. `true`
  means it cannot; the cursor ends after this outcome. When given, the terminal `span` starts at the
  first byte not yet returned, so the caller can recover the rest.
- **Strictness.** Formats with an end marker (json array/object, xml, avro, parquet, length-prefixed
  protobuf, every compression format except snappy, see 4.1) report truncation as `Failed`, never
  as a clean end. Trailing content after a JSON root, or after the last compressed stream, is an
  error.
- **`CORRUPT_COMPRESSION` is its own kind**, because it may mean a file that was still being written.
  The caller decides whether to drop or retry.

### 3.4 Limits

- `maxRecordBytes` bounds one record. Formats the decoder frames itself (line, multiline, json
  lines, csv, length-prefixed protobuf) enforce it exactly. Library-parsed formats (json array,
  xml elements, yaml multi-document) enforce it with 64 KiB of slack for parser read-ahead.
- `maxDocumentBytes` bounds a unit held whole: a whole document, a decompressed avro block, a
  parquet row group. Avro and parquet sizes are checked before allocating.
- An oversized record keeps only a 4 KiB preview; the rest is read, counted and dropped. Line-based
  formats, csv and avro blocks continue with the next record or block (csv stops only when a quote
  stays open to the end); the others stop (`terminal=true`). xml elements and yaml multi-document
  cannot locate a record, so their `Oversized` has no preview and no span; an avro block's
  `Oversized` has neither, and its `sizeSoFar` is the block's size, what it decompressed to before
  the limit, the memory xz needs, or 0 for a zstd window. A codec over its memory limit gives a
  terminal `Oversized` with no span or preview; `sizeSoFar` is the memory xz needs, or 0 for zstd,
  which does not say.
- Parser-internal limits (Jackson string/name length, YAML code points, XML attribute length) are
  lifted so that only these two limits decide. Nesting depth keeps its default and is `MALFORMED`
  when exceeded.

### 3.5 Raw bytes and positions

- `recordIndex`: physical line number for line framing (first line for multiline), otherwise the
  record ordinal starting at 1.
- `span` is the record's offset and length in the stream after decompression and transcoding. It is
  given where exact (line-based formats, csv, json array elements, whole documents) and null
  otherwise (xml elements, yaml multi-document, avro, parquet).
- `Failed.raw` holds the record's bytes where they can be located; otherwise it is empty and the
  caller uses `span`, or fetches the input again.

### 3.6 Extending

`FormatConfig` and `FramingConfig` are sealed interfaces registered with `@JsonSubTypes` and compiled
in one switch, so a missing case fails to compile. A format implements `FormatFactory` /
`FormatCursor`; error classification is never repeated in a format.

## 4. Configuration

```jsonc
{
  "compression": ["gzip"],              // optional; a string or a list; applied in order; "auto" alone
  "format": { "type": "json", "mode": "lines" }
}
```

### 4.1 Compression

| Codec | Notes |
|---|---|
| `gzip` | Concatenated members are all read; trailing garbage is `CORRUPT_COMPRESSION` |
| `auto` | Detects gzip only; anything else passes through uncompressed |
| `deflate` | The zlib format (RFC 1950); concatenated streams are all read; a stream that needs a preset dictionary is `CORRUPT_COMPRESSION` |
| `zstd` | Concatenated and skippable frames; input that ends inside a frame is `CORRUPT_COMPRESSION`. A frame whose window needs more than zstd's default 128 MiB is `Oversized` |
| `bzip2` | Concatenated streams |
| `xz` | Concatenated streams and stream padding. A stream that needs more memory than `maxDocumentBytes` (in whole KiB) is `Oversized`; without that limit, the cap is 128 MiB or half the heap, whichever is smaller |
| `lz4` | The LZ4 frame format only, with skippable frames; not the raw block format, and not frames that need a dictionary. A block without a checksum is handed on as it decodes; a block with one is held until it matches, so up to one block (at most 4 MiB) can be lost when it does not match or is cut short |
| `snappy` | The Snappy framing format only, with skippable chunks; not raw or Hadoop snappy. The format has no end mark, so input cut exactly between chunks ends cleanly; only the `ByteInput` contract (3.2) catches that |

Every codec follows the same rules: concatenated streams are all read, bytes after the last stream
that do not start another one are `CORRUPT_COMPRESSION`, and input with no bytes decodes to nothing.
gzip and deflate hand on everything they decoded before damaged data. The other codecs are read at
most 256 bytes at a time, because a library that fails drops what it decoded in that call, so up to
255 bytes decoded just before the damage can be lost.

Compression inside avro and parquet belongs to those formats and is not configured here.

### 4.2 Formats

| `type` | Parameters | Record |
|---|---|---|
| `json` | `mode`: `object`, `array`, `lines` | Each record must be an object. UTF-16/UTF-32 input is detected (RFC 4627 §3) and transcoded |
| `text` | `charset` (default UTF-8); `framing`: `line` (`skipBlank`, default true), `multiline` (`startPattern` or `continuePattern`, `maxLines`), `whole` | `{"__raw__": "<text>"}`. Lines split on LF, CRLF, CR; one leading BOM is removed |
| `csv` | `delimiter` (default `,`), `quote` (default `"`, `""` turns quoting off), `escape`, `header` (default true), `columns`, `skipLines`, `commentMarker`, `ignoreSurroundingSpaces` (default false), `charset` | One object per row; values are strings |
| `xml` | `mode`: `document`, or `elements` with `recordPath` (e.g. `/Events/Event`) | Any DTD is refused; values are strings |
| `yaml` | `mode`: `document`, `multi_document` | Each document must be a mapping |
| `avro` | `container`: `ocf` (an object container file, which carries its schema), or `datum` (the whole input is one record) with `schema` (JSON, or a string holding it) | One record per avro record; the schema must be a record |
| `protobuf` | `descriptorSet`, `messageType`, `framing`: `single`, `varint_delimited` | One record per message |
| `parquet` | — | Needs random access; cannot be combined with `compression` |

**Multiline.** Exactly one pattern is set; it is a Java regex searched anywhere in the line, so
anchor it with `^`. With `startPattern`, a matching line starts a new event and other lines join the
current one. With `continuePattern`, a matching line joins the current event and other lines start a
new one. The first line of the input always starts an event, so lines before the first start line,
or a leading continuation line, form an event of their own. Blank lines follow the same rule: with
`startPattern` they stay in the current event, and with `continuePattern` they start a new one
unless the pattern matches them. An event keeps its inner line endings as they were. It is emitted
when the next event starts or the input ends. Events made only of blank lines are skipped. An event
over `maxLines` or `maxRecordBytes` is reported whole as `Oversized`, even if it is blank, and the
lines that continue it are counted into it, so a stack trace never splits into records that look
valid. An oversized line is matched by its 4 KiB preview.

**CSV.** Records follow RFC 4180: they end at LF, CRLF or CR outside quotes; a quoted field may hold
delimiters and line breaks, and `""` inside it is one quote. A quote inside an unquoted field is a
literal character. `escape`, when set, makes the next character literal anywhere, including a line
break. The single-character options must be ASCII, differ from each other and not be CR or LF; an
empty `escape` or `commentMarker` means off. `ignoreSurroundingSpaces` trims spaces and tabs (except
the delimiter) around fields and outside quotes.

Names come from `columns` if given, otherwise from the header line when `header` is true, otherwise
`_c0`, `_c1`, … (rows may then differ in length). An empty header name becomes `_c<i>`. With
`header` true and `columns` given, the header line is skipped without being checked. `skipLines`
drops physical lines before anything else, ignoring quotes. Empty lines and lines starting with
`commentMarker` are skipped; a line of only spaces is a row.

Text after a closing quote (other than ignored spaces) is `MALFORMED`, as are a row whose column
count differs from the names and a row over 16,384 columns (Excel's limit; each field costs far more
memory than its bytes). Decoding continues with the next row. A quote left open until the end of the
input is terminal. When names come from the header line, a header that is malformed, oversized or
has duplicate names is terminal, reported with `recordIndex` 0 and a span to the end of the input;
data rows count from 1.

**XML.** Elements map the way Jackson's `XmlMapper` maps them: attributes and child elements become
fields, repeated children become an array, an empty element is `""`, and text next to attributes or
children goes under the key `""`. In `document` mode only comments, processing instructions and
whitespace may follow the root. In `elements` mode `recordPath` is absolute (each step an XML name),
matched by local name with namespaces ignored; other elements are skipped, and a non-empty input
where it matches nothing is `MALFORMED`. Any `DOCTYPE` is `MALFORMED` and terminal, so no entity is
expanded and nothing is fetched. The encoding comes from the XML declaration or byte order mark.

**YAML.** YAML 1.1 as SnakeYAML reads it (so `yes`/`no` are booleans and `012` is octal). Aliases
are expanded into separate copies, up to one value per two bytes of the size limit (the most an
alias-free document within the limit could hold), so aliases never make a record larger than the
limit allows (without a limit, aliases may copy at most 1,000,000 values, since their growth is
exponential); entries copied by merge keys (`<<`) count against the same cap, and copies of one
scalar share its text; a document that contains itself is `MALFORMED`. Duplicate keys are
`MALFORMED`, and so are a mapping key that is not a scalar and two keys that become the same name
(`1` and `'1'`). Nesting is limited to 500 levels, in the source and after alias expansion, to stay
clear of stack overflows in the recursive parser. An empty or null document gives no record. In
`multi_document` mode a document that fails to construct (not a mapping, duplicate keys, an alias
loop, an invalid date such as `2024-02-30`, a value that does not fit its tag, an unknown local tag
such as `!foo`, nesting too deep after alias expansion) is `MALFORMED` and decoding goes on; a
syntax error, too deep nesting in the source, or a global tag such as `!!python/object` is terminal.
The encoding comes from the byte order mark (UTF-8 without one).

**Avro.** Object container files are parsed here, not by avro's `DataFileStream`, which allocates
whatever size a block header claims and takes a cut-off file for a clean end. A block's size is
checked against `maxDocumentBytes` before it is read, and its decompressed size while it is
decompressed (xz's own memory is capped at 128 MiB or half the heap, whichever is smaller, and a
zstd window over 128 MiB is oversized too). Blocks are framed, so an oversized block (`Oversized`),
a damaged compressed block (`CORRUPT_COMPRESSION`), a block claiming more records than it can hold
(which counts as one record, since its count cannot be trusted), or a record that does not decode
(`MALFORMED`, including one nested too deeply for the thread stack) loses only the records of its
block, and decoding goes on with the next block; record numbers still count the lost records. A
record that decodes but cannot be converted loses only itself, and bytes left after a block's
records are reported against the block's first record. A wrong sync marker or a file that does not
end exactly after a block is terminal. Every string, bytes and fixed length is checked against the
bytes left in the block before anything is allocated. Two budgets bound what one block can make the
reader do: allocation counts (records in the block, array and map items) may total its byte length
plus 100,000, since only nulls, empty records and empty fixeds encode in no bytes; and the objects
built (a record and its fields, array items, map keys and values) may total 16 per byte plus
1,000,000. A block over either budget loses its records; a block with more than 100,000 records that
encode in no bytes is one of them. The file header may be up to `maxDocumentBytes` or 16 MiB,
whichever is larger. In `datum` mode, bytes left after the record are `MALFORMED`, and an empty
input decodes if the schema fits in zero bytes. Codecs: null, deflate, snappy, zstandard, bzip2, xz.

**Charsets.** Text in any other charset is transcoded to UTF-8 first. Unmarked `UTF-16` and `UTF-32`
take their byte order from a byte order mark and default to big-endian. Bytes that are invalid in
the charset become U+FFFD.

Examples: a Java stack trace is `text` + `multiline` with `continuePattern: "^(\\s|Caused by: )"`;
syslog, CEF and key=value are `text` + `line`, parsed downstream by the `parser` node.

### 4.3 Type mapping (binary formats and YAML)

| Source | Record value |
|---|---|
| integers, float, double | number (int64 above 2^53 loses precision; accepted) |
| unsigned integers | the unsigned value, never negative |
| decimal | string |
| bytes, fixed, YAML `!!binary` | base64 string |
| YAML mapping key that is not a string | its string form; a date key becomes its number |
| YAML null | null |
| YAML integer outside int64 | its decimal string |
| YAML `.inf`, `-.inf`, `.nan`; avro float and double infinities and NaN | the strings `Infinity`, `-Infinity`, `NaN` (JSON has no such numbers) |
| avro uuid (on string or a 16-byte fixed) | its string form |
| null items of an array, null values of a map | null (only null record fields are left out) |
| enum | string |
| timestamp | epoch milliseconds (local timestamps, and YAML timestamps without a zone, read as UTC) |
| date | days since 1970-01-01 |
| time of day, protobuf `Duration` | milliseconds |
| map, record, struct, message | object |
| array, repeated, list | array |
| unset protobuf field with presence; null union | omitted |
| unset proto3 scalar without presence | its default value |

## 5. Delivery

Each step is one PR into `feature/generic-decoder`; the branch merges into `main` at the end.

| PR | Scope | Status |
|---|---|---|
| 1 | API, config and compiler, error classification, budgets, `gzip` + `auto`, `json` (all modes), `text` `line` + `whole` (UTF-8), `lowMemoryTest` task | Done (#339) |
| 2 | `text` `multiline`, non-UTF-8 text | Done (#340) |
| 3 | `csv` | Done (#341) |
| 4 | `xml`, `yaml` | Done (#342) |
| 5 | Remaining codecs | Done (#343) |
| 6 | `avro` | Done (#344) |
| 7 | `protobuf` | Planned |
| 8 | `parquet`, random-access input | Planned |

## 6. Testing

- Every outcome described above has a fixture: empty input, wrong or corrupt compression, truncation,
  trailing content, non-object records, oversized records at and one byte past each limit.
- A read failure injected at any point (inside a gzip header, between members, mid-record) must throw
  `SourceReadException`, however the parser wraps it.
- `lowMemoryTest` runs with a 64 MiB heap: generated inputs of hundreds of MB decode completely, and
  oversized records come back as `Oversized`, not OOM. It is excluded from `test` and wired into
  `check`.
- Tests compare whole expected outcome lists, not field by field.

## 7. Known limitations

- The decoder is not used by any source yet.
- Damaged compressed data can decode into wrong but well-formed bytes before the codec notices, for
  every codec. A stream's records are only confirmed once its checksum, if it has one, matches.
- zstd, bzip2, xz, lz4 and snappy can lose up to 255 decoded bytes that came just before damaged
  data, and lz4 a whole block whose block checksum fails (see 4.1).
- For non-UTF-8 input, `raw`, `span` and the limits refer to the transcoded UTF-8 bytes, not the
  original.
- A multiline event is held until the next event starts. When the input then fails, the event is not
  emitted; the terminal outcome's `span` starts at its first byte.
- An oversized record in a library-parsed format ends decoding of that input, because the start of
  the next record cannot be found.
- xml elements and yaml multi-document have empty `raw`, no `span` and no preview; avro and parquet
  records have no bytes of their own.
- Tree formats (json array and object, xml, yaml) hold a record as a tree that can take 10–50 times
  its bytes in memory; only csv caps the number of fields.
- An avro block may build at most 16 objects per byte (4.2). Records with about 15 or more bare
  `null` columns per payload byte, or wrapper records nested 8 or more deep around a one-byte value,
  exceed it and lose their block. In the worst case an avro block holds about 150–250 times
  `maxDocumentBytes` in memory, about twice what ordinary data of that size can reach.
- Recursive parsers (Jackson, SnakeYAML) need a normal thread stack (1 MiB or more) to reach their
  nesting limits. On a smaller stack, deep input can overflow it; the cursor then reports a terminal
  `MALFORMED` instead of letting the `StackOverflowError` escape.
- In a json array, more than 64 KiB of whitespace between elements counts toward the neighbouring
  record's budget.

## 8. PR1 implementation notes

Decisions made while implementing PR1 that refine the design above.

| # | Note |
|---|---|
| P3 | gzip is our own `Inflater(nowrap)` loop with our own header/trailer parsing. The JDK and commons-compress readers either swallow errors at member boundaries or drop already-inflated bytes when they fail |
| P5 | Non-UTF-8 JSON is transcoded by our own stream, which hands on each source read immediately and replaces only the single bad code unit, so a later failure cannot lose converted bytes |
| P6 | Bytes inflated before a corrupt spot are recovered through `Inflater.getBytesWritten()` and returned before the error |
| P8 | Line framing enforces its budget itself and continues; the shared budget flag is only for library-parsed formats, whose `Oversized` is always terminal |
| P9 | Content after a json array's closing `]` is `MALFORMED` |
| P10 | `skipBlank` skips empty lines and lines of only spaces or tabs |
| P11 | Small API conveniences: `DecodeOutcome.terminal()`, `RawSpan.toEnd`, negative limits rejected, config names case-insensitive |
| P12 | gzip headers are capped at 1 MiB (a legal FEXTRA is up to 64 KiB) |
| P13 | The json array whitespace limitation in section 7 |
| P14 | The flags are checked when a format throws, ends, or returns a terminal outcome; decoded records and non-terminal failures pass through unchanged |
| P15 | Formats are registered through sealed config types and one compiler switch, not ServiceLoader |
| P16 | The gzip "inflater made no progress" branch cannot be reached under the JDK contract and is not tested. The transcoder's no-progress guard is tested with a stub charset (PR2) |

## 9. PR2 implementation notes

| # | Note |
|---|---|
| P17 | UTF-32 is decoded by our own decoder. The JDK decoders drop a leading byte order mark on their own and pass surrogate code points through, so D800 followed by DC00 became one valid character. Now each becomes U+FFFD, and exactly one leading U+FEFF is dropped for every charset. A second U+FEFF is kept as text |
| P18 | Unmarked `UTF-16` and `UTF-32` are resolved to a fixed byte order from the first bytes before decoding. `x-UTF-16LE-BOM` is handled like unmarked `UTF-16` but defaults to little-endian. The JDK UTF-32 charsets that drop a mark themselves (`X-UTF-32BE-BOM`, `X-UTF-32LE-BOM`) are replaced by their fixed-order forms. So no decoder drops a mark |
| P19 | Line framing skips a UTF-8 byte order mark only on untranscoded input |
| P20 | Malformed input skips the length the charset decoder reports. UTF-16 is the exception: it skips one code unit, because the JDK reports a lone high surrogate together with the unit after it. A pair cut off by the end of the input is one sequence |

## 10. PR3 implementation notes

| # | Note |
|---|---|
| P21 | CSV is scanned by our own byte-level parser, not commons-csv. All special characters are ASCII, so scanning UTF-8 bytes is safe. This gives every row an exact `span` and raw bytes, an exact `maxRecordBytes`, and an oversized row that is skipped to its end instead of ending the input. commons-csv only reports character positions, buffers ahead and has no per-record limit |
| P22 | The header options are `header` (boolean) and `columns` (names) instead of `header: first_line \| none \| [names]`. It mirrors what FLE-2796 asks of the CSV serializer (an explicit column list and a header on/off switch), so reading and writing can use the same option names |
| P23 | Quoting is turned off with `"quote": ""`, because an explicit JSON null cannot be told apart from an absent field |
| P24 | RFC 4180 quoting by default: without `ignoreSurroundingSpaces`, a space before an opening quote makes the field unquoted, and a space after a closing quote is `MALFORMED` |
| P25 | Rows are capped at 16,384 columns. A row of delimiters under `maxRecordBytes` costs about 200 bytes of heap per field, so without a cap one row could exhaust memory |

## 11. PR4 implementation notes

| # | Note |
|---|---|
| P26 | YAML uses SnakeYAML directly instead of Jackson's YAML reader, which turns aliases into the anchor's name and cannot tell `2024-01-15` from `"2024-01-15"`. A date-only timestamp becomes days, so it is not taken for a timestamp at midnight UTC |
| P27 | Woodstox is created by name, because service lookup picks a shaded StAX implementation from the Hadoop client on this classpath |
| P28 | A DTD is refused as soon as it is seen, rather than parsed with DTD support off: there is then no partial DTD processing to reason about |
| P29 | xml elements and yaml multi-document restart their budget at the bytes read so far after each record (and, for xml, after each event outside a record), because both parsers buffer ahead and give no byte offsets. The limit is therefore approximate by up to one parser buffer, and `Oversized` has no span or preview |
| P30 | Woodstox, Jackson XML and SnakeYAML come in through the existing Jackson XML and YAML dependencies; no new dependency is declared |
| P31 | SnakeYAML's own alias limit (50 per stream) is lifted because it counts across documents and would end a valid stream; the size-based cap in `YamlRecords` replaces it, plus a cap on values copied through aliases when no limit is set, and a depth check after expansion. Any failure while constructing a composed document is reported per document, so it never ends a multi-document stream |
| P32 | Woodstox's limit of 1,000 attributes per element is lifted, like the attribute length, so only the decoder's limits decide |
| P33 | Merge-key chains are counted while SnakeYAML flattens mappings, against the same value cap, because they grow quadratically before `YamlRecords` sees them. The text of a shared scalar (`!!binary`, big integers) is made once per object, so alias copies share it |

## 12. PR5 implementation notes

| # | Note |
|---|---|
| P34 | zstd uses its own loop over zstd-jni's streaming context. zstd-jni's input stream reports a clean end when the last of several frames is cut short |
| P35 | bzip2 streams are chained here, one library stream each. commons-compress's concatenated mode looks for the next stream inside the read that ends the previous one, so a failure there loses the previous stream's tail |
| P36 | Library codecs are read 256 bytes at a time. In a local measurement on 40 MB of log lines this cost under 5% of throughput, while byte-by-byte reads, which would lose nothing, ran at a third to three quarters of the speed |
| P37 | The xz memory limit is `maxDocumentBytes`, not `max(maxDocumentBytes, 128 MiB)`: with a floor the caller's limit could never apply. Without `maxDocumentBytes` it is 128 MiB (zstd's default window limit) or half the heap, whichever is smaller, so a tiny file declaring a huge dictionary cannot exhaust the heap. zstd's own limit cannot be configured through zstd-jni's context and stays at 128 MiB. Going over it, or over zstd's window limit, is reported through the budget flag as `Oversized`, not as corrupt data |
| P38 | commons-compress, zstd-jni and `org.tukaani:xz` are declared in `lib`; the first two were only transitive before |
| P39 | lz4 uses its own frame loop; commons-compress only decodes block contents. Its frame reader ends cleanly when the input stops right after an uncompressed block's size. Skippable frames are skipped by reading, because `FileInputStream.skip` goes past the end of a file without saying so |
| P40 | A library codec leaves a failure alone when another layer already marked it, so an outer codec's memory limit stays `Oversized` through inner codecs |
| P41 | ZlibStream's "inflater made no progress" branch cannot be reached under the JDK contract and is not tested, like gzip's (P16) |

## 13. PR6 implementation notes

| # | Note |
|---|---|
| P42 | Avro object container files are parsed here; avro's `DataFileStream` allocates the size a block header claims, decompresses without a bound, and treats a cut-off file as a clean end |
| P43 | A bad avro block or record is not terminal. The earlier plan ended an avro file at its first bad record and at its first oversized block; block boundaries are known, so decoding now goes on with the next block |
| P44 | Records are decoded with our own bounded `Decoder` over the block bytes instead of avro's `BinaryDecoder`, whose length limits are JVM-wide system properties |
| P45 | Avro comes in through the `api` module's `api` dependency; nothing new is declared |
| P46 | Avro allocation counts draw on one budget per block instead of being checked one array block at a time, which rejected valid arrays of nulls near the end of a block and let many small array blocks claim a quadratic number of items. The objects built are budgeted per byte as well, because a count alone does not bound a record with many fields that encode in no bytes; charging each zero-byte value as it was read, as tried in review, rejected valid files with a few null columns. A block's record count is checked against its compressed size only for the null codec. `fixed` sizes, which come from the file's schema, are checked by a `GenericDatumReader` subclass before avro allocates them |
