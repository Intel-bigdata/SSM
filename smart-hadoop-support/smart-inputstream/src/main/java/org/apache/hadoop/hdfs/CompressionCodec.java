/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.bzip2.Bzip2Compressor;
import org.apache.hadoop.io.compress.bzip2.Bzip2Decompressor;
import org.apache.hadoop.io.compress.bzip2.Bzip2Factory;
import org.apache.hadoop.io.compress.lz4.Lz4Compressor;
import org.apache.hadoop.io.compress.lz4.Lz4Decompressor;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.util.NativeCodeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * This class decide which compressor type for SmartCompressorStream 
 */
public class CompressionCodec {
  static final Logger LOG = LoggerFactory.getLogger(CompressionCodec.class);
  public static final String LZ4 = "Lz4";
  public static final String BZIP2 = "Bzip2";
  public static final String SNAPPY = "snappy";
  public static final String ZLIB = "Zlib";
  public static final List<String> CODEC_LIST = Arrays.asList(LZ4, BZIP2, SNAPPY, ZLIB);

  private static Configuration conf = new Configuration();
  private static boolean nativeCodeLoaded = NativeCodeLoader.isNativeCodeLoaded();

  public static boolean getNativeCodeLoaded() {
    return nativeCodeLoaded;
  }

  /**
   * Return compression overhead of given codec
   * @param bufferSize   buffSize of codec (int)
   * @param codec        codec name (String)
   * @return compression overhead (int)
   */
  public static int compressionOverhead(int bufferSize, String codec) {
    // According to Hadoop 3.0
    switch (codec) {
      case LZ4:
        return bufferSize / 255 + 16;
      case SNAPPY:
        return bufferSize / 6 + 32;
      default:
        return 18;
    }
  }

  /**
   *  Create a compressor
   */
  public static Compressor createCompressor(int bufferSize, String codec)
      throws IOException {

    if (!CODEC_LIST.contains(codec)) {
      throw new IOException("Invalid compression codec, SSM only support: " +
          CODEC_LIST.toString());
    }
    if (!codec.equals(ZLIB) && !nativeCodeLoaded) {
      throw new IOException(codec + " is not supported, " +
          " because Hadoop native lib was not successfully loaded");
    }

    // Sequentially load compressors
    switch (codec) {
      case LZ4:
        if (Lz4Codec.isNativeCodeLoaded()) {
          return new Lz4Compressor(bufferSize);
        }
        throw new IOException("Failed to load/initialize native-Lz4 library");

      case BZIP2:
        if (Bzip2Factory.isNativeBzip2Loaded(conf)) {
          return new Bzip2Compressor(Bzip2Factory.getBlockSize(conf),
              Bzip2Factory.getWorkFactor(conf),
              bufferSize);
        }
        throw new IOException("Failed to load/initialize native-bzip2 library");

      case SNAPPY:
        if (SnappyCodec.isNativeCodeLoaded()) {
          return new SnappyCompressor(bufferSize);
        }
        throw new IOException("Failed to load/initialize native-snappy library");

      case ZLIB:
        if (nativeCodeLoaded) {
          return new ZlibCompressor(ZlibCompressor.CompressionLevel.DEFAULT_COMPRESSION,
              ZlibCompressor.CompressionStrategy.DEFAULT_STRATEGY,
              ZlibCompressor.CompressionHeader.DEFAULT_HEADER,
              bufferSize);
        }
        // TODO buffer size for build-in zlib codec
        return ZlibFactory.getZlibCompressor(conf);

      default:
        throw new IOException("Unsupported codec: " + codec);
    }
  }

  /**
   *  Create a Decompressor
   */
  public static Decompressor creatDecompressor(int bufferSize, String codec) throws IOException {

    if (!CODEC_LIST.contains(codec)) {
      throw new IOException("Invalid compression codec, SSM only recognize: " +
          CODEC_LIST.toString());
    }

    if (!codec.equals(ZLIB) && !nativeCodeLoaded) {
      throw new IOException("Hadoop native lib was not successfully loaded, so " +
          codec + " is not supported.");
    }

    // Sequentially load a decompressor
    switch (codec) {
      case LZ4:
        if (Lz4Codec.isNativeCodeLoaded()) {
          return new Lz4Decompressor(bufferSize);
        }
        throw new IOException("Failed to load/initialize native-Lz4 library");

      case BZIP2:
        if (Bzip2Factory.isNativeBzip2Loaded(conf)) {
          return new Bzip2Decompressor(false, bufferSize);
        }
        throw new IOException("Failed to load/initialize native-bzip2 library");

      case SNAPPY:
        if (SnappyCodec.isNativeCodeLoaded()) {
          return new SnappyDecompressor(bufferSize);
        }
        throw new IOException("Failed to load/initialize native-snappy library");


      case ZLIB:
        if (nativeCodeLoaded) {
          return new ZlibDecompressor(
              ZlibDecompressor.CompressionHeader.DEFAULT_HEADER, bufferSize);
        }
        // TODO buffer size for build-in zlib codec
        return ZlibFactory.getZlibDecompressor(conf);
      default:
        throw new IOException("Unsupported codec: " + codec);
    }
  }
}
