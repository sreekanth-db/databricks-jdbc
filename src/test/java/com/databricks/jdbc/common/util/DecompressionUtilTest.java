package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.common.CompressionCodec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DecompressionUtilTest {
  private static final String CONTEXT = "testContext";
  private static final String INITIAL_STRING = "testData";
  private static InputStream compressedInputStream;

  private static DecompressionUtil decompressionUtil = new DecompressionUtil();

  @BeforeAll
  public static void setCompressedInputStream() throws IOException {
    byte[] uncompressedData = INITIAL_STRING.getBytes();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (LZ4FrameOutputStream lz4FrameOutputStream =
        new LZ4FrameOutputStream(byteArrayOutputStream)) {
      lz4FrameOutputStream.write(uncompressedData);
    }
    compressedInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
  }

  @Test
  public void testDecompressLZ4Frame() throws Exception {
    InputStream resultStream =
        decompressionUtil.decompress(compressedInputStream, CompressionCodec.LZ4_FRAME, CONTEXT);
    assertNotNull(resultStream, "The decompressed stream should not be null.");
    assertTrue(
        IOUtils.contentEquals(resultStream, new ByteArrayInputStream(INITIAL_STRING.getBytes())));
  }

  @Test
  public void testDecompressLZ4FrameSkipsCompression() throws Exception {
    assertEquals(
        decompressionUtil.decompress(compressedInputStream, CompressionCodec.NONE, CONTEXT),
        compressedInputStream);
    assertNull(
        DecompressionUtil.decompress(
            (ByteArrayInputStream) null, CompressionCodec.LZ4_FRAME, CONTEXT));
  }
}
