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
| `serdes.des` and its subpackages (csv, jsonarr, jsonobj, jsonobjline, strline, text, xml) | 34 |
| Decompressors in `serdes.compression`: `Decompressor`, `DecompressorFactory`, `CompositeDecompressor`, `GzipDecompressor`, `GzipDetectingDecompressor`, `NoopDecompressor` | 6 |
| `commands/source/BytesRawDataConverter`, `commands/sqssource/SqsRawDataConverter`, `commands/pubsubsource/PubSubRawDataConverter`, `commands/azureiothubsource/IotHubRawDataConverter` | 4 |

Kept:
- The compressors in `serdes.compression` (`Compressor`, `CompressorFactory`, `GzipCompressor`,
  `NoopCompressor`): the S3 and blob sinks write with them; they are not decoders.
- `serdes.converters` (`TypedEventConverter` and its implementations): the serializers use them.
- `commands/source/RawDataConverter`, the interface the generic converters implement.
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

The message is the same text as `DeserializerFactory`'s, so every source's validation error, and
every caller or test that matches it, stays the same. The set is private: no caller needs it, and
tests name the seven values themselves.

### IoT Hub

`IotHubMessageConverter` and `IotHubMetadataExtractor` use `IotHubRawDataConverter`'s constants
(`ENVELOPE_KEY`, `DEVICE_ID_KEY`, `ENQUEUED_TIME_KEY`, `PROPERTIES_KEY`, `PROPERTY_PREFIX`) and its
static `buildEnvelope`. They move, unchanged, into `IotHubMessageConverter`, the converter the source
uses; `IotHubMetadataExtractor` reads them from there.

## Behavior changes

None for sources, sinks or their configuration: the generic decoder already does all decoding, and
the validation text is kept. The change is to the library's public API: the removed classes are gone
for any code that compiled against them.

## Tests

Removed with their classes:
- the tests under `serdes/des` (7 files) and the decompressor tests in `serdes/compression`
  (`CompositeDecompressorTest`, `DecompressorFactoryTest`, `GzipDecompressorTest`,
  `GzipDetectingDecompressorTest`); `GzipCompressorTest` and `CompressorFactoryTest` stay;
- `BytesRawDataConverterTest` and `IotHubRawDataConverterTest`. The latter's three envelope cases
  (metadata nested under `_iothub` with the body fields kept; empty `properties` when there are no
  application properties; no envelope without metadata) move to `IotHubMessageConverterTest`
  through `IotHubMessageConverter`, unless a case there already covers them.

Rewritten without the legacy side, keeping every case:
- Differential tests (`kafkasource/MessageDecodingDifferentialTest`,
  `s3realtimesource/LegacyDifferentialTest`, the legacy comparison in `IotHubMessageConverterTest`
  and in `ElasticsearchRawDataConverterTest`): each case already states the expected result of each
  side. The generic decoder's result is compared with its stated expectation; the legacy column and
  helpers go. A case that only records a legacy difference keeps its generic-decoder expectation.
- Tests that use a legacy class as a tool:
  - `ActiveMqSourceFetcherTest`, `KafkaSourceFetcherTest`: build the converter the source builds
    (`MessageDecodingConverter`) instead of `BytesRawDataConverter`.
  - `SimpleSourceCommandTest`, `StdInSourceFetcherTest`: mock `RawDataConverter` (or use
    `MessageDecodingConverter`) instead of a mocked `FleakDeserializer`.
  - `ParserCommandTest`, `CompiledRulesTest`: read their line fixtures without the STRING_LINE
    deserializer (plain line reading), same expected objects.
  - `S3SinkCommandTest`: read back the written objects with the generic decoder for the same
    `encodingType` (`SourceDecoding` + `RecordDecoder`), same expected objects.
  - `FileSourceConfigValidatorTest`, `KafkaSourceConfigValidatorTest`,
    `StdInSourceConfigValidatorTest`: iterate the seven accepted values listed in the test instead
    of `DeserializerFactory.SUPPORTED_ENCODING_TYPES`.
  - `S3DecodingTest`: its expected message no longer concatenates
    `DeserializerFactory.SUPPORTED_ENCODING_TYPES`; it checks the fixed prefix
    ("Unsupported deserialization encoding type: PARQUET. Supported types:"), since the set's
    order is not stable.
  - `KvTypedSerializerTest`: the assertion that `DeserializerFactory` rejects KV goes;
    `SourceDecodingTest.invalidCombinationsAreRejected` gains the KV case next to PARQUET.

Checks:
- `git grep -nE 'serdes\.des|DeserializerFactory|FleakDeserializer|Decompressor|BytesRawDataConverter|SqsRawDataConverter|PubSubRawDataConverter|IotHubRawDataConverter' -- . ':!docs'`
  finds nothing.
- `./gradlew build` (all modules) and `./gradlew :lib:goldenTest` pass; the golden harness uses no
  removed class, and no expected file changes.

## Docs

`docs/spec/*.md` that describe a removed class as current code are updated to say it was removed
(historical comparisons with the legacy decoder stay as history).

## Delivery

One PR into main. The next release carries the API removal; code that still uses a removed class
must move to `SourceDecoding` / `RecordDecoder` / `MessageDecodingConverter` first.
