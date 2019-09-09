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

import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
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
import org.smartdata.conf.SmartConf;

/**
 * This class decide which compressor type for SmartCompressorStream 
 */
public class CompressionCodec {
  static final Logger LOG = LoggerFactory.getLogger(CompressionCodec.class);
  private String hadoopNativePath;
  SmartConf conf = new SmartConf();

  public CompressionCodec() {
    // hadoopNativePath is used to support Bzip2
    if (System.getenv("HADOOP_HOME") != null) {
      this.hadoopNativePath = System.getenv("HADOOP_HOME") + "/lib/native/libhadoop.so";
    } else if (System.getenv("HADOOP_COMMON_HOME") != null){
      this.hadoopNativePath = System.getenv("HADOOP_COMMON_HOME") + "/lib/native/libhadoop.so";
    }
    if (hadoopNativePath != null) {
      System.load(hadoopNativePath);
    }
  }

  /**
   * Return compression overhead of given codec
   * @param bufferSize buffSize of codec (int)
   * @param compressionImpl codec name (String)
   * @return compression overhead (int)
   */
  public int compressionOverhead(int bufferSize, String compressionImpl) {
    // According to Hadoop 3.0
    switch (compressionImpl) {
      case "Lz4":
        return bufferSize / 255 + 16;
      case "snappy" :
        return bufferSize / 6 + 32;
      default:
        return 18;
    }
  }

  /**
   *  Create a compressor
   */
  public Compressor createCompressor(int bufferSize, String compressionImpl) {
    // Sequentially load compressors
    switch (compressionImpl) {
      case "Lz4" :
        if (NativeCodeLoader.isNativeCodeLoaded()) {
          return new Lz4Compressor(bufferSize);
        } else {
          LOG.error("Failed to load/initialize native-lz4 library");
        }

      case "Bzip2" :
        if (NativeCodeLoader.isNativeCodeLoaded()) {
          if (Bzip2Factory.isNativeBzip2Loaded(conf)) {
            return new Bzip2Compressor(Bzip2Factory.getBlockSize(conf),
              Bzip2Factory.getWorkFactor(conf),
              bufferSize);
          } else {
            LOG.error("Failed to load/initialize native-bzip2 library");
          }
        }

      case "snappy" :
        if (NativeCodeLoader.isNativeCodeLoaded()) {
          if (SnappyCodec.isNativeCodeLoaded()) {
            return new SnappyCompressor(bufferSize);
          } else {
            LOG.error("Failed to load/initialize native-snappy library");
          }
        }

      default:
        if (NativeCodeLoader.isNativeCodeLoaded()) {
          return new ZlibCompressor(ZlibCompressor.CompressionLevel.DEFAULT_COMPRESSION,
              ZlibCompressor.CompressionStrategy.DEFAULT_STRATEGY,
              ZlibCompressor.CompressionHeader.DEFAULT_HEADER,
              bufferSize);
        } else {
          // TODO buffer size for build-in zlib codec
          return ZlibFactory.getZlibCompressor(conf);
        }
    }
  }

  /**
   *  Create a Decompressor
   */
  public Decompressor creatDecompressor(int bufferSize, String compressionImpl){
    // Sequentially load decompressors
    switch (compressionImpl){
      case "Lz4" :
        if (NativeCodeLoader.isNativeCodeLoaded()) {
          return new Lz4Decompressor(bufferSize);
        } else {
          LOG.error("Failed to load/initialize native-lz4 library");
        }

      case "Bzip2" :
        if (NativeCodeLoader.isNativeCodeLoaded()) {
          if (Bzip2Factory.isNativeBzip2Loaded(conf)) {
            return new Bzip2Decompressor(false, bufferSize);
          } else {
            LOG.error("Failed to load/initialize native-bzip2 library");
          }
        }

      case "snappy" :
        if (NativeCodeLoader.isNativeCodeLoaded()) {
          if (SnappyCodec.isNativeCodeLoaded()) {
            return new SnappyDecompressor(bufferSize);
          } else {
            LOG.error("Failed to load/initialize native-snappy library");
          }
        }

      default:
        if (NativeCodeLoader.isNativeCodeLoaded()) {
          return new ZlibDecompressor(
              ZlibDecompressor.CompressionHeader.DEFAULT_HEADER, bufferSize);
        } else {
          // TODO buffer size for build-in zlib codec
          return ZlibFactory.getZlibDecompressor(conf);
        }
    }
  }
}
