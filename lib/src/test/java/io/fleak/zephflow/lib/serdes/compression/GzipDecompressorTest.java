package io.fleak.zephflow.lib.serdes.compression;

import io.fleak.zephflow.lib.serdes.SerializedEvent;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

public class GzipDecompressorTest {

    @Test
    public void testDecompress() throws IOException {
        var originalText = "Hello, this is a test string!";
        byte[] compressedData = compress(originalText);

        var decompressor = new GzipDecompressor();
        var event = decompressor.decompress(
                new SerializedEvent(null, compressedData, Map.of())
        );
        var result = new String(event.value(), StandardCharsets.UTF_8);

        assertEquals(originalText, result);
    }

    private byte[] compress(String str) throws IOException {
        var bos = new ByteArrayOutputStream();
        try (var gzip = new GZIPOutputStream(bos)) {
            gzip.write(str.getBytes(StandardCharsets.UTF_8));
        }
        return bos.toByteArray();
    }
}
