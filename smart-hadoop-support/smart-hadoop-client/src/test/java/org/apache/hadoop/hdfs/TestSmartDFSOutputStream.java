package org.apache.hadoop.hdfs;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.BlockCompressorStream;
import org.apache.hadoop.io.compress.BlockDecompressorStream;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.conf.SmartConf;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Random;

import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Created by root on 10/27/17.
 */
public class TestSmartDFSOutputStream {
  private MiniDFSCluster cluster;
  private SmartConf conf;

  @Before
  public void setup() throws Exception {
    System.out.println(System.getProperty("java.library.path"));
    assumeTrue(SnappyCodec.isNativeCodeLoaded());
  }

  @Test
  public void test() {
    int BYTE_SIZE = 1024 * 100;
    byte[] bytes = BytesGenerator.get(BYTE_SIZE);
    int bufferSize = 262144;
    int compressionOverhead = (bufferSize / 6) + 32;
    DataOutputStream deflateOut = null;
    DataInputStream inflateIn = null;
    try {
      DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
      CompressionOutputStream deflateFilter = new BlockCompressorStream(
          compressedDataBuffer, new SnappyCompressor(bufferSize), bufferSize,
          compressionOverhead);
      deflateOut = new DataOutputStream(new BufferedOutputStream(deflateFilter));

      deflateOut.write(bytes, 0, bytes.length);
      deflateOut.flush();
      deflateFilter.finish();

      DataInputBuffer deCompressedDataBuffer = new DataInputBuffer();
      deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0,
          compressedDataBuffer.getLength());

      CompressionInputStream inflateFilter = new BlockDecompressorStream(
          deCompressedDataBuffer, new SnappyDecompressor(bufferSize),
          bufferSize);

      inflateIn = new DataInputStream(new BufferedInputStream(inflateFilter));

      byte[] result = new byte[BYTE_SIZE];
      inflateIn.read(result);

      Assert.assertArrayEquals(
          "original array not equals compress/decompressed array", result,
          bytes);
    } catch (IOException e) {
      fail("testSnappyCompressorDecopressorLogicWithCompressionStreams ex error !!!");
    } finally {
      try {
        if (deflateOut != null)
          deflateOut.close();
        if (inflateIn != null)
          inflateIn.close();
      } catch (Exception e) {
      }
    }
  }

  static final class BytesGenerator {
    private BytesGenerator() {
    }

    private static final byte[] CACHE = new byte[] { 0x0, 0x1, 0x2, 0x3, 0x4,
        0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF };
    private static final Random rnd = new Random(12345l);

    public static byte[] get(int size) {
      byte[] array = (byte[]) Array.newInstance(byte.class, size);
      for (int i = 0; i < size; i++)
        array[i] = CACHE[rnd.nextInt(CACHE.length - 1)];
      return array;
    }
  }
}
