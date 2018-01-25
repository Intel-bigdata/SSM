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
package org.smartdata.hdfs;

import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.bzip2.Bzip2Compressor;
import org.apache.hadoop.io.compress.bzip2.Bzip2Decompressor;
import org.apache.hadoop.io.compress.bzip2.Bzip2Factory;
import org.apache.hadoop.io.compress.lz4.Lz4Compressor;
import org.apache.hadoop.io.compress.lz4.Lz4Decompressor;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor;
import org.apache.hadoop.util.NativeCodeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.SmartAction;
import org.smartdata.conf.SmartConf;

/**
 * This class decide which compressor type for SmartCompressorStream 
 */
public class CompressionCodec {
  static final Logger LOG = LoggerFactory.getLogger(SmartAction.class);
  private String hadoopnativePath;
  SmartConf conf = new SmartConf();

  public CompressionCodec() {
    //hadoopnativePath used to suport Bzip2 compresionImpl 
    if (!(System.getenv("HADOOP_HOME") == null)) {
      this.hadoopnativePath = System.getenv("HADOOP_HOME") + "/lib/native/libhadoop.so";
    }else {
      this.hadoopnativePath = System.getenv("HADOOP_COMMON_HOME") + "/lib/native/libhadoop.so";
    }
    System.load(hadoopnativePath);
  }

  /**
   *  Create a compressor
   */
  public Compressor createCompressor(int bufferSize, String compressionImpl) {
    switch (compressionImpl) {
      case "Lz4" :
        return  new Lz4Compressor(bufferSize);

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

      case "Zlib" :
        return new ZlibCompressor(ZlibCompressor.CompressionLevel.DEFAULT_COMPRESSION,
          ZlibCompressor.CompressionStrategy.DEFAULT_STRATEGY,
          ZlibCompressor.CompressionHeader.DEFAULT_HEADER,
          bufferSize);

      default:
        return new SnappyCompressor(bufferSize);
    }
  }

  /**
   *  Create a Decompressor
   */
  public Decompressor creatDecompressor(int bufferSize, String compressionImpl){
    switch (compressionImpl){
      case "Lz4" :
        return  new Lz4Decompressor(bufferSize);

      case "Bzip2" :
        if (NativeCodeLoader.isNativeCodeLoaded()) {
          if (Bzip2Factory.isNativeBzip2Loaded(conf)) {
            return new Bzip2Decompressor(false, bufferSize);
          } else {
            LOG.error("Failed to load/initialize native-bzip2 library");
          }
        }

      case "Zlib" :
        return new ZlibDecompressor(ZlibDecompressor.CompressionHeader.DEFAULT_HEADER,bufferSize);

      default:
        return new SnappyDecompressor(bufferSize);
    }
  }
}
