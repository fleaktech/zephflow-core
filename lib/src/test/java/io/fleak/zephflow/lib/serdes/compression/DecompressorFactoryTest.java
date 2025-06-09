package io.fleak.zephflow.lib.serdes.compression;

import io.fleak.zephflow.lib.serdes.CompressionType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DecompressorFactoryTest {

    @Test
    public void testGetDecompressorWithNullType() {
        var decompressor = DecompressorFactory.getDecompressor((CompressionType) null);
        assertTrue(decompressor instanceof NoopDecompressor, "Expected NoopDecompressor for null input");
    }

    @Test
    public void testGetDecompressorWithGzip() {
        var decompressor = DecompressorFactory.getDecompressor(CompressionType.GZIP);
        assertTrue(decompressor instanceof GzipDecompressor, "Expected GzipDecompressor for GZIP input");
    }

    @Test
    public void testGetDecompressorWithNullList() {
        var decompressor = DecompressorFactory.getDecompressor((List<CompressionType>) null);
        assertTrue(decompressor instanceof NoopDecompressor, "Expected NoopDecompressor for null list input");
    }

    @Test
    public void testGetDecompressorWithEmptyList() {
        var decompressor = DecompressorFactory.getDecompressor(Collections.emptyList());
        assertTrue(decompressor instanceof NoopDecompressor, "Expected NoopDecompressor for empty list input");
    }

    @Test
    public void testGetDecompressorWithSingleTypeList() {
        var decompressor = DecompressorFactory.getDecompressor(List.of(CompressionType.GZIP));
        assertTrue(decompressor instanceof CompositeDecompressor, "Expected CompositeDecompressor for list input");
    }

    @Test
    public void testGetDecompressorWithMultipleTypes() {
        var decompressor = DecompressorFactory.getDecompressor(List.of(CompressionType.GZIP, CompressionType.GZIP));
        assertTrue(decompressor instanceof CompositeDecompressor, "Expected CompositeDecompressor for multiple compression types");
    }
}
