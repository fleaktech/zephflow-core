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
  failure are returned first.
- **`terminal`.** `false` means the next record can still be found and decoding continues. `true`
  means it cannot; the cursor ends after this outcome. When given, the terminal `span` starts at the
  first byte not yet returned, so the caller can recover the rest.
- **Strictness.** Formats with an end marker (json array/object, xml, avro, parquet,
  length-prefixed protobuf, every compression format) report truncation as `Failed`, never as a
  clean end. Trailing content after a JSON root, or after the last compressed stream, is an error.
- **`CORRUPT_COMPRESSION` is its own kind**, because it may mean a file that was still being written.
  The caller decides whether to drop or retry.

### 3.4 Limits

- `maxRecordBytes` bounds one record. Formats the decoder frames itself (line, multiline, json
  lines, csv, length-prefixed protobuf) enforce it exactly. Library-parsed formats (json array,
  xml elements, yaml multi-document) enforce it with 64 KiB of slack for parser read-ahead.
- `maxDocumentBytes` bounds a unit held whole: a whole document, a decompressed avro block, a
  parquet row group. Avro and parquet sizes are checked before allocating.
- An oversized record keeps only a 4 KiB preview; the rest is read, counted and dropped. Line-based
  formats and csv continue with the next record (csv stops only when a quote stays open to the end);
  the others stop (`terminal=true`).
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
| `zstd`, `snappy`, `lz4`, `bzip2`, `xz`, `deflate` | Planned. snappy and lz4: standard framing format only. deflate is zlib (RFC 1950) |

Compression inside avro and parquet belongs to those formats and is not configured here.

### 4.2 Formats

| `type` | Parameters | Record |
|---|---|---|
| `json` | `mode`: `object`, `array`, `lines` | Each record must be an object. UTF-16/UTF-32 input is detected (RFC 4627 §3) and transcoded |
| `text` | `charset` (default UTF-8); `framing`: `line` (`skipBlank`, default true), `multiline` (`startPattern` or `continuePattern`, `maxLines`), `whole` | `{"__raw__": "<text>"}`. Lines split on LF, CRLF, CR; one leading BOM is removed |
| `csv` | `delimiter` (default `,`), `quote` (default `"`, `""` turns quoting off), `escape`, `header` (default true), `columns`, `skipLines`, `commentMarker`, `ignoreSurroundingSpaces` (default false), `charset` | One object per row; values are strings |
| `xml` | `mode`: `document`, or `elements` with `recordPath` | DTDs and external entities are disabled |
| `yaml` | `mode`: `document`, `multi_document` | Each document must be a mapping |
| `avro` | `container`: `ocf` (schema from the file), `datum` (schema in config) | One record per avro record |
| `protobuf` | `descriptorSet`, `messageType`, `framing`: `single`, `varint_delimited` | One record per message |
| `parquet` | — | Needs random access; cannot be combined with `compression` |

**Multiline.** Exactly one pattern is set; it is a Java regex searched anywhere in the line, so
anchor it with `^`. With `startPattern`, a matching line starts a new event and other lines join the
current one. With `continuePattern`, a matching line joins the current event and other lines start
a new one. The first line of the input always starts an event, so lines before the first start
line, or a leading continuation line, form an event of their own. Blank lines follow the same rule:
with `startPattern` they stay in the current event, and with `continuePattern` they start a new one
unless the pattern matches them. An event keeps its inner line endings as they were. It is emitted
when the next event starts or the input ends. Events made only of blank lines are skipped. An event over `maxLines` or
`maxRecordBytes` is reported whole as `Oversized`, even if it is blank, and the lines that continue
it are counted into it, so a stack trace never splits into records that look valid. An oversized
line is matched by its 4 KiB preview.

**CSV.** Records follow RFC 4180: they end at LF, CRLF or CR outside quotes; a quoted field may hold
delimiters and line breaks, and `""` inside it is one quote. `escape`, when set, makes the next
character literal in or out of quotes. The single-character options must be ASCII. Names come from
`columns` if given, otherwise from the header line when `header` is true, otherwise `_c0`, `_c1`, …;
an empty header name becomes `_c<i>`. With `header` true and `columns` given, the header line is
skipped. `skipLines` drops physical lines before anything else, ignoring quotes. Empty lines and
lines starting with `commentMarker` are skipped. Text after a closing quote (other than ignored
spaces) is `MALFORMED`, as is a row whose column count differs from the names; decoding continues
with the next row. A quote left open until the end of the input is terminal. A header that is
malformed, oversized or has duplicate names is terminal and reported with `recordIndex` 0; data rows
count from 1.

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
| enum | string |
| timestamp | epoch milliseconds (local timestamps read as UTC) |
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
| 3 | `csv` | In review |
| 4 | `xml`, `yaml` | Planned |
| 5 | Remaining codecs | Planned |
| 6 | `avro` | Planned |
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
- Damaged compressed data can inflate into wrong but well-formed bytes before zlib notices. A gzip
  member's records are only confirmed once its trailer CRC matches.
- For non-UTF-8 input, `raw`, `span` and the limits refer to the transcoded UTF-8 bytes, not the
  original.
- A multiline event is held until the next event starts. When the input then fails, the event is not
  emitted; the terminal outcome's `span` starts at its first byte.
- An oversized record in a library-parsed format ends decoding of that input, because the start of
  the next record cannot be found.
- xml elements and yaml multi-document have empty `raw`; avro and parquet records have no bytes of
  their own.
- In a json array, more than 64 KiB of whitespace between elements counts toward the neighbouring
  record's budget.
- A bad record in the middle of an avro file ends decoding; there is no resync at the next block.

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
| P24 | Strict RFC 4180 by default: without `ignoreSurroundingSpaces`, a space before an opening quote makes the field unquoted, and a space after a closing quote is `MALFORMED` |
