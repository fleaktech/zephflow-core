# Elasticsearch Source: Records Straight from `_source`

The `elasticsearchsource` command (`ElasticsearchSourceCommand`) scrolls an index. For each hit, the
fetcher parses `_source` into a `Map`; the converter then writes that `Map` back to JSON bytes,
decodes the bytes with the legacy `FleakDeserializer` of the required `encodingType`, and puts the
hit's `_id` and `_index` over each record's fields as strings. This change makes the converter
build the record from the `Map` directly, makes the fetcher measure each `_source`'s size in the
response, and makes `encodingType` optional and unused.

## 1. Why

- **No decoding to do.** Elasticsearch stores every document as a JSON object, and the search
  response returns `_source` as one. The fetcher has already parsed it, so writing it back to
  bytes and parsing them again only costs time. The bytes are also what the size counter counts;
  the fetcher can measure them in the response it already reads instead.
- **`encodingType` has no useful value.** JSON_OBJECT and JSON_OBJECT_LINE give the same record.
  Every other value gives a result no one would want: TEXT and STRING_LINE give the document's JSON
  text in `__raw__`; CSV reads that text as a header line, so it gives no records or fails; XML and
  JSON_ARRAY fail every hit.
- **Legacy code.** This is the last source command in this repository that decodes with a legacy
  deserializer. After it, no source command decodes with one; the unused legacy converter classes
  and the `encodingType` check in `SourceDecoding` still reference `serdes.des`.

## 2. Goals and non-goals

**Goals**
- Each hit's `_source` becomes the record directly, whatever `encodingType` is set to. Building
  the record and counting its size never write `_source` back to JSON.
- For an existing JSON_OBJECT or JSON_OBJECT_LINE configuration, the records are the same as today
  (section 4 lists the one exception).
- The command's code no longer uses `serdes.des`.
- Existing configurations still parse and validate.

**Non-goals**
- The fetcher's requests (scroll, query, batch size, timeouts, auth, clearing the scroll), its
  constructor, the batch source type and its no-commit strategy, and how the counters are
  computed: unchanged. Their values change only as section 4 lists.
- The raw bytes for the DLQ and for raw-data samples: `ElasticsearchRawDataEncoder` keeps writing
  `_source` as compact JSON, so a DLQ entry or a sample is byte for byte as today. This happens on
  a failure and once per sampling interval, not for every hit.
- New fields, `_source` filtering, other hit fields (`_score`, `fields`, `_routing`) in the record.
- Removing `encodingType` from the config: a config parser that meets an unknown field fails, so
  configs that set it would stop parsing.

## 3. Design

### 3.1 Fetcher: reading hits

`ElasticsearchSourceFetcher` keeps receiving each response as a string (`BodyHandlers.ofString`,
which decodes by the response's charset and replaces malformed input, as today).
`parseScrollResponse` builds the same tree as today's `OBJECT_MAPPER.readTree(body)`, and while
building it records where each hit's `_source` is in the body:

- A streaming `JsonParser` from `OBJECT_MAPPER`'s factory reads the body. The root object, the
  root's `hits` object, its `hits` array, and each object in that array are built field by field
  into `ObjectNode`s and `ArrayNode`s (`set` keeps the last of duplicate fields, as `readTree`
  does). Every other value is read whole with
  `OBJECT_MAPPER.readerFor(JsonNode.class).without(FAIL_ON_TRAILING_TOKENS)`, so its node is the
  one `readTree` builds. A body whose first token is not `{` is read with `readTree(body)`, as
  today.
- For a `_source` field of an object in that array, the parser's character offset at the start of
  the value and after it are recorded, keyed by the value's node. The source size is the UTF-8
  length of that part of the body. Only the node that ends up in the tree is used, so the last of
  duplicate `_source` fields, or of duplicate `hits` fields, counts.
- After the root object, the next token must be the end of input; anything else fails as
  `readTree` does today (`FAIL_ON_TRAILING_TOKENS`).
- The rest of `parseScrollResponse` is unchanged and runs on the finished tree: it reads
  `_scroll_id`, decides whether the scroll is exhausted and clears it, and builds each
  `ElasticsearchDocument` with `path("_id").asText()`, `path("_index").asText()` and
  `convertValue(_source, Map.class)` for an object `_source`, otherwise an empty `Map`.
  The document's `sourceSize` is the recorded size of its `_source`, 0 when it has none.

A probe of this design over bodies with non-ASCII text, whitespace, scalar, `null` and nested
`_source` values (including inner `hits` and `_scroll_id` keys), `_id` as `null` and a number, a
non-object element, `_scroll_id` after `hits`, duplicate `hits` and `_source` fields, a non-array
`hits.hits`, and trailing content gave a tree equal to `readTree(body)` and the exact sizes.

`ElasticsearchDocument` gains a `long sourceSize` component. Its three-argument constructor stays
(size 0), so code that builds a document without a size still compiles.

### 3.2 Converter: record and counters

For each hit, `ElasticsearchRawDataConverter` builds one record:

1. `FleakData.wrap(source)`.
2. `_id` and `_index` are put over the record's fields as strings, an absent one as `""`, as
   today. A `_source` field named `_id` or `_index` is replaced, as today.

`FleakData.wrap` turns the `Map`'s values into the same `FleakData` as the legacy JSON paths do:
integers that fit a `long` become `LONG` numbers, other numbers (decimals, and integers beyond a
`long`) become `DOUBLE`, `null` stays `null`, and arrays and objects nest. This was measured
against the JSON_OBJECT legacy path, starting from the fetcher's own parsing, for small, long, and
beyond-long integers (including a 400-digit one); decimals, `1.0`, exponents, subnormals,
`1e-400` and `-0.0`; nulls, booleans, non-ASCII strings, nested and empty arrays and objects;
duplicate field names (the last one wins on both paths); an empty and a non-object `_source`; and
`_id` and `_index` clashes. `FleakData.wrap` copies the `Map`, so the document the DLQ encoder
later writes is not changed.

Counters:

- `dataSizeCounter` counts the document's `sourceSize`, the UTF-8 length of `_source` as it appears
  in the response, instead of the length of `_source` written back as JSON (section 4).
- `inputEventCounter` counts one record per hit, as today.
- Both keep today's tags, `getCallingUserTagAndEventTags(null, record)`, now taken from the new
  record (its `__tag__` field).
- A conversion that throws keeps today's handling: `deserializeFailureCounter` goes up, the error is
  logged, and the hit fails with `ConvertedResult.failure`, so the source's DLQ handling and the
  raw bytes from `ElasticsearchRawDataEncoder` are unchanged. With no parsing left, only an
  unexpected error reaches this path.

### 3.3 Configuration

- `encodingType` loses `@NonNull` and is no longer read. It stays in `ElasticsearchSourceDto.Config`
  and still accepts every `EncodingType` name, so an existing config parses as before.
- The validator no longer calls `DeserializerFactory.validateEncodingType`. The other checks
  (`host`, `index`, `batchSize`, the credential lookup) are unchanged.
- The command no longer creates a deserializer; `ElasticsearchRawDataConverter` loses its
  constructor argument.

## 4. Behavior changes for existing configurations

| Configuration or input | Today | Now |
|---|---|---|
| `encodingType` TEXT or STRING_LINE | `{"__raw__": "<_source as JSON text>"}` plus `_id`, `_index` | the `_source` record |
| `encodingType` CSV | no records, or the hit fails (the JSON text is read as a CSV header line); `inputEventCounter` 0 for the hit | the `_source` record; `inputEventCounter` 1 |
| `encodingType` XML or JSON_ARRAY | every hit fails: `deserializeFailureCounter` 1, `inputEventCounter` and `dataSizeCounter` not raised | the `_source` record; `inputEventCounter` 1 and `dataSizeCounter` raised |
| `encodingType` PARQUET or KV | the config fails validation | the config is accepted; the `_source` record |
| no `encodingType` | the config fails (`NullPointerException`) | the config is accepted |
| `encodingType` TEXT, STRING_LINE, CSV, XML or JSON_ARRAY: counter tags | none (the record has no `__tag__`, or there is no record) | the tags of the `_source`'s `__tag__` field |
| a `_source` number outside the `double` range, e.g. `1e400` | the string `"Infinity"` (or `"-Infinity"`) | a `DOUBLE` number, infinite |
| `dataSizeCounter` | the length of `_source` written back as compact JSON | the length of `_source` as it appears in the response |

The `1e400` row: the fetcher parses such a number as an infinite `double`, and writing it back as
JSON gives the string `"Infinity"`, which the legacy path then read as a string. The string came
from the round trip, not from the document, so the change keeps the parsed number.

The `dataSizeCounter` row: Elasticsearch returns `_source` as it was indexed (an index with a
synthetic `_source` returns a rebuilt one). The two lengths are equal for compact JSON and differ
for whitespace between tokens, escapes such as `\u00e9`, and number spellings such as `1e2` (written
back as `100.0`). A hit with no `_source` counts 0 bytes, and one with a non-object `_source`
(possible only in a hand-made response) counts that value's length, such as 4 for `null`; both
counted 2 (`{}`) before.

## 5. Compatibility

- **Config:** every config that parses today still parses; `encodingType` is optional.
- **Classes:** `ElasticsearchRawDataConverter`'s constructor takes no argument.
  `ElasticsearchDocument` gains `sourceSize` and keeps its three-argument constructor. The
  fetcher's constructor and `fetch()` are unchanged.
- **Records:** unchanged for JSON_OBJECT and JSON_OBJECT_LINE except the `1e400` row of section 4.

## 6. Testing

There are no tests for this source today.

- **Fetcher tree:** for a corpus of response bodies (the cases of the probe in 3.1 and the
  documents of 3.2), the tree the fetcher builds equals `OBJECT_MAPPER.readTree(body)`, and each
  document's `(id, index, source)` equals what today's code builds from `readTree(body)` (kept in
  the test as the reference), with number types compared through `unwrap()`. `sourceSize` equals
  the UTF-8 length of the `_source` text in the body; trailing content fails as today.
- **Differential:** for the same documents, today's path (the JSON_OBJECT and the JSON_OBJECT_LINE
  legacy deserializers over the `_source` written as JSON, with `_id` and `_index` as metadata)
  against the new converter. Records are compared whole as their `unwrap()` maps, so number types
  count; `1e400` is pinned as the listed difference.
- **Command:** the command driven against a stub HTTP server that answers the initial search, one
  scroll page and an empty page. The stub records each request; the test checks the method, path,
  query string, body and `Authorization` header of the search, the scroll and the scroll clearing
  (a `DELETE` carrying the scroll id was received, without counting them, since the fetcher also
  clears on close). It checks the records in order as `unwrap()` maps, `dataSizeCounter` and
  `inputEventCounter` with their tags for a `_source` with a `__tag__` field, and that a non-200
  answer fails the fetch with today's message. The config with no `encodingType`, and with TEXT,
  gives the same records.
- **Validator:** a config without `encodingType`, and one with PARQUET, passes; a blank host or
  index or a `batchSize` of 0 fails as today.
- `./gradlew :lib:goldenTest` stays green (the source has no golden fixture).

## 7. Delivery

On the integration branch `feature/elasticsearch-source`: PR 0 this spec, PR 1 the change and its
tests. The branch merges to main after PR 1, with approval. The UI form and the docs, which still
ask for `encodingType`, are updated separately.
