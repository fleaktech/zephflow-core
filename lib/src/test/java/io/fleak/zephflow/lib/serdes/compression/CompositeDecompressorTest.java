package io.fleak.zephflow.lib.serdes.compression;

import io.fleak.zephflow.lib.serdes.SerializedEvent;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CompositeDecompressorTest {

    private byte[] gzip(byte[] input) throws IOException {
        var bos = new ByteArrayOutputStream();
        try (var gzipOut = new GZIPOutputStream(bos)) {
            gzipOut.write(input);
        }
        return bos.toByteArray();
    }

    @Test
    public void testNestedGzipDecompression() throws IOException {
        var originalText = "hello world";
        var onceCompressed = gzip(originalText.getBytes(StandardCharsets.UTF_8));
        var twiceCompressed = gzip(onceCompressed);

        var event = new SerializedEvent(null, twiceCompressed, Map.of("level", "2x"));

        var decompressor = new CompositeDecompressor(
                List.of(new GzipDecompressor(), new GzipDecompressor())
        );

        var result = decompressor.decompress(event);
        var decompressedText = new String(result.value(), StandardCharsets.UTF_8);

        assertEquals(originalText, decompressedText);
    }
}
