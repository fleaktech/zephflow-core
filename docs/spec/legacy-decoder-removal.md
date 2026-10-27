# Removing the legacy decoders

## Why

Every source now decodes with the generic decoder (`serdes.decode`, `RecordDecoder`), through
`SourceDecoding` and `MessageDecodingConverter`. The legacy decoding code is still public API next to
it:

- the deserializers in `serdes.des`, including `FleakDeserializer` and `DeserializerFactory`;
- the decompressors in `serdes.compression`;
- four converters that no source creates any more: `BytesRawDataConverter`, `SqsRawDataConverter`,
  `PubSubRawDataConverter` and `IotHubRawDataConverter`.

There are two decoders with different results for the same input, and a caller that picks the old
one no longer gets what the sources do. Nothing outside this repository's tests uses them any more,
so they can go.

## Scope

Removed (main code):

| What | Files |
|------|-------|
| `serdes.des` and its subpackages (csv, jsonarr, jsonobj, jsonobjline, strline, text, xml) | 24 |
| Decompressors in `serdes.compression`: `Decompressor`, `DecompressorFactory`, `CompositeDecompressor`, `GzipDecompressor`, `GzipDetectingDecompressor`, `NoopDecompressor` | 6 |
| `commands/source/BytesRawDataConverter`, `commands/sqssource/SqsRawDataConverter`, `commands/pubsubsource/PubSubRawDataConverter`, `commands/azureiothubsource/IotHubRawDataConverter` | 4 |
| The decode half of `serdes.converters`, used only by the deserializers: `JsonObjectTypedEventConverter`, `StringTypedEventConverter`; `TypedEventConverter.typedEventToFleakData` / `payloadToFleakData` and their implementations in `ObjectMapTypedEventConverter` and `RecordFleakDataTypedEventConverter`; `TypedEventContainer.getMetadataPayload` | 2 files, 4 edited |

The converters row is a second step past the ticket's list: those methods convert legacy
deserializer output and have no other caller here or in the downstream code checked for this change,
so keeping them would leave a public half of the legacy decode path.

Kept:
- The compressors in `serdes.compression` (`Compressor`, `CompressorFactory`, `GzipCompressor`,
  `NoopCompressor`): the S3 and blob sinks write with them; they are not decoders.
- The encode half of `serdes.converters` (`fleakDataToTypedEvent`) and `TypedEventContainer`: the
  serializers use them. Its `metadataWithKey` component is always null once the deserializers are
  gone (only they set it), but `SingleEventTypedSerializer` reads it; it stays so that the
  serializers' API does not change here.
- `commands/source/RawDataConverter`, the interface the generic converters implement.
- General utilities that library users call directly: `XmlUtils`, `JsonUtils.fromJsonBytes`,
  `CompressionUtils.gunzip` / `isGzip`. Inside this repository only the removed code and tests call
  them, but they are public helpers in `utils`, not part of the legacy decoders.
- `EncodingType`, `CompressionType`, the serializers, and every source's configuration.

Out: any change to what a source accepts or produces; the serializers; `EncodingType` values (KV and
PARQUET stay for the sinks).

## Design

### `SourceDecoding`

`decodingConfig()` validates `encodingType` with `DeserializerFactory.validateEncodingType` today.
The accepted set and the check move into `SourceDecoding` unchanged:

```java
private static final Set<EncodingType> SUPPORTED_ENCODING_TYPES =
    Set.of(CSV, JSON_OBJECT, JSON_ARRAY, JSON_OBJECT_LINE, STRING_LINE, TEXT, XML);
...
if (!SUPPORTED_ENCODING_TYPES.contains(encodingType))
  throw new IllegalArgumentException(
      "Unsupported deserialization encoding type: "
          + encodingType
          + ". Supported types: "
          + SUPPORTED_ENCODING_TYPES);
```

The message is the same text as `DeserializerFactory`'s, so every source's validation error stays the
same. The set is private: no caller needs it, and tests name the seven values themselves. The
`format()` branches for PARQUET and KV stay unreachable, as today.

### IoT Hub

`IotHubMessageConverter` and `IotHubMetadataExtractor` use `IotHubRawDataConverter`'s constants
(`ENVELOPE_KEY`, `DEVICE_ID_KEY`, `ENQUEUED_TIME_KEY`, `PROPERTIES_KEY`, `PROPERTY_PREFIX`) and its
static `buildEnvelope`. They move, unchanged, into `IotHubMessageConverter`, the converter the source
uses; `IotHubMetadataExtractor` reads them from there. `IotHubMessageConverter`'s class Javadoc stops
linking to the removed class (the build runs javadoc).

## Behavior changes

None for sources, sinks or their configuration: the generic decoder already does all decoding, and
the validation text is kept. The change is to the library's public API: the removed classes and
methods are gone for any code that compiled against them.

## Tests

Removed with their code:
- the tests under `serdes/des` (7 files), with the resources only they read
  (`serdes/json_array_per_line_input.jsonl`, `serdes/unittest_xml_input.xml`,
  `serdes/unittest_xml_expected_output.json`), and the decompressor tests in `serdes/compression`
  (`CompositeDecompressorTest`, `DecompressorFactoryTest`, `GzipDecompressorTest`,
  `GzipDetectingDecompressorTest`); `GzipCompressorTest` and `CompressorFactoryTest` stay. The
  `isGzip` cases of `GzipDetectingDecompressorTest` (null, empty, one byte) move to a new
  `CompressionUtilsTest`, since `isGzip` stays public;
- `BytesRawDataConverterTest`;
- `IotHubRawDataConverterTest`: its three cases are already covered, on whole records through the
  source's converter, by `IotHubMessageConverterTest`'s `metadataAndProperties`,
  `metadataWithoutProperties` and `noMetadata`;
- `ObjectMapTypedEventConverterTest`: it tests only the removed `typedEventToFleakData`.

Legacy comparisons, rewritten to keep every case:
- `kafkasource/MessageDecodingDifferentialTest`, `s3realtimesource/LegacyDifferentialTest` and the
  comparison in `IotHubMessageConverterTest`: every case already states the generic decoder's
  expected result. That result is compared with its stated expectation; the legacy column, its
  helpers and the legacy-only expectations go. The classes become `MessageDecodingCasesTest` and
  `S3RealtimeDecodingCasesTest`; method names and Javadocs that mention the legacy side
  (`legacyAndDecoderResults`, `aTruncatedGzipEmitsTheRecordsBeforeTheCutWhereLegacyFailed`,
  `legacyAndDecoderGiveTheSameRecords`, `aBadMessageFailsOnBoth`,
  `xmlWithOver1021AttributesNowDecodes`, the case builder `legacy(...)`) are renamed to say what the
  generic decoder does.
- `ElasticsearchRawDataConverterTest.recordsAreTheLegacyJsonOnes` states no expectation: its 10
  `SOURCES` documents (big and small numbers, `-0.0`, duplicate keys, `_id`/`_index` clashes) are
  compared with the legacy deserializers' output computed at run time. Before the legacy code goes,
  that output is captured once, on this branch, and written into the test as a literal expected
  record list per document; the test then compares the converter's records with it. The captured
  values are what the converter produces today (the test passes now), so no expectation changes;
  the test is renamed (`recordsKeepTheirJsonTypes`). `aNumberBeyondTheDoubleRangeStaysANumber`
  also calls the legacy helper: its two legacy rows go and its converter row (positive and negative
  infinity) stays as the whole expected value.

Tests that use a legacy class as a tool:
- `ActiveMqSourceFetcherTest`, `KafkaSourceFetcherTest`: build the converter the source builds
  (`MessageDecodingConverter`) instead of `BytesRawDataConverter`, with the same input bytes and
  expected records.
- `SimpleSourceCommandTest`: `MessageDecodingConverter` with real JSON bytes replaces
  `BytesRawDataConverter` around a mocked `FleakDeserializer`. The counter verifies stay (that
  converter owns `dataSizeCounter`, `inputEventCounter`, `deserializeFailureCounter`), with the byte
  sizes of the new inputs; the failing case uses bytes that are not JSON, and its DLQ check looks for
  the generic decoder's `MALFORMED` text instead of "Test deserialization error".
- `StdInSourceFetcherTest`: the mocked `FleakDeserializer` and its stubs are deleted; the fetcher
  never receives it.
- `ParserCommandTest`, `CompiledRulesTest`: the line fixtures are read with plain line reading,
  each line wrapped as `{"__raw__": line}` as STRING_LINE did, and the JSON fixtures (JSON_ARRAY, and JSON_OBJECT in
  `testParseKvWithTabSeparator`) with `JsonUtils.fromJsonBytes` into `ObjectNode`s and
  `JsonUtils.fromJsonPayload` per object, the mapping the legacy JSON deserializers used, so the
  expected objects are unchanged.
- `S3SinkCommandTest`: read back the written objects with the generic decoder for the same
  `encodingType` and no decompression (`MessageDecodingConverter.decodingConfig(encodingType, null,
  false)` + `RecordDecoder`), so an uncompressed sink that starts compressing still fails; the gzip
  case keeps its explicit `CompressionUtils.gunzip` first. Same expected objects.
- `FileSourceConfigValidatorTest`, `KafkaSourceConfigValidatorTest`,
  `StdInSourceConfigValidatorTest`: iterate the seven accepted values listed in the test instead of
  `DeserializerFactory.SUPPORTED_ENCODING_TYPES`.
- `S3DecodingTest`: its expected message no longer concatenates
  `DeserializerFactory.SUPPORTED_ENCODING_TYPES`. It checks the fixed prefix
  ("Unsupported deserialization encoding type: PARQUET. Supported types: ") and that the bracketed
  list, parsed into a set, equals the seven names (the set's order is not stable).
- `KvTypedSerializerTest`: the assertion that `DeserializerFactory` rejects KV goes, and
  `kvIsOptInPerSinkAndNotDeserializable` is renamed to what it still checks;
  `SourceDecodingTest.invalidCombinationsAreRejected` gains the KV case next to PARQUET.

Checks:
- `git grep -nP 'serdes\.des\b|DeserializerFactory|FleakDeserializer|\b(Composite|Gzip|GzipDetecting|Noop)?Decompressor(Factory)?\b|(Bytes|Sqs|PubSub|IotHub)RawDataConverter|(JsonObject|String)TypedEventConverter|typedEventToFleakData|payloadToFleakData|getMetadataPayload' -- . ':!docs'`
  finds nothing (`-P`: `\b` needs Perl syntax; before the change it lists every file this spec
  removes or rewrites). (`serdes.decode.compress.Decompressors` and the parquet codecs' decompressors are
  generic-decoder code and do not match.)
- `./gradlew build` (all modules, javadoc included) and `./gradlew :lib:goldenTest` pass; the golden
  harness and fixtures use no removed class, and no expected file changes.

## Docs

`docs/ARCHITECTURE.md` stops describing `lib/serdes/` as holding deserializers (the generic decoder
lives in `lib/serdes/decode/`), and `MessageDecodingConverter`'s Javadoc stops comparing with "the
legacy deserializers". Other docs under `docs/spec` and `docs/superpowers` are records of
earlier work and stay as written.

## Delivery

One PR into main. The next release carries the API removal; code that still uses a removed class
must move to `SourceDecoding` / `RecordDecoder` / `MessageDecodingConverter` first.
