

Create Enum DecompressorType
Create DecompressorInterface
   (SerializedEvent) returns SerliazedEvent
        
Create DecompressorFactory
        returns a CompositeDecompressor that decompressor from an array of types

Update KinesisSourceCommandPartsFactory::createRawDataConverter
   to call the factory and return a composite decompress and pass to the BytesRawDataConverter

Update BytesRawDataConverter 
  to have two constructors one produces a default NOOP decompressor
  the other takes a decompressor which kinesis source cmd sets.

  this class before converting the serialized object does an decompress on the bytes.


Update KinesisSourceCommand's config to take in a list of decompressor types.


--- 

 Serdes 
   Encoding -> Fleak Object
 
   EncodingFormats are ways to write objects.
    CSV -> Object
    JSON -> Object

   WrapperEncoding:
     this goes from bytes to bytes, it unwraps protocol layers
     like TCP layers text / ssl 
     Gzip -> byte - byte wrapper
     Compression

  SerialisedEvent
      contains bytes 


   Serdes:
     go from SerialisedEvent.bytes -> Object
              SerialisedEvent
                unwrapBytes        -> Object
                  ifgzip decode ... 


  Producers of SerliazedEvents
    Source Commands:
         SerializedEvents originates here:
         config: { user knows if its wrapped in gzip or not, 
            gzip ( base64 ( gzip ( json )) )
          }
            
      bts = bytes
      for unwrapper in unwrappers:
            bts = unwrapper.unwrap(bts)

      EncodingType
      WrapperType
      CompressionType

    1. SourceInitialiseConfig 
            we need an array of CompressionTypes
            default is null
            each source command code can set this.
    2. Factory to select the correct decompressor from the compression type
    3. 

TODO: 
  remove the gzip guessing from kinesis source
  and replace with allowing the encoding type to be an array.
  Update FleakSerdes to take a list of encoding types and then update all the subclasses to work with that
  in the single and multi events deser decode the data using the encoding types left to right.

  Then work on the json config so we are flexible to accept a list but this happens on each source's dto config.
   Apply this in all source commands.
  Then replicate the same for all sinks.


FleakSerdes ->
JsonArrayDeserializerFactory
CsvDeserializerFactory
StringLineDeserializerFactory
JsonObjectLineDeserializerFactory
JsonObjectDeserializerFactory

