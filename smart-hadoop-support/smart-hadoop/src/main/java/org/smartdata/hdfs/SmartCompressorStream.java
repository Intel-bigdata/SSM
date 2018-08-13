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
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.smartdata.model.CompressionFileState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * SmartOutputStream.
 */
public class SmartCompressorStream {

  private Compressor compressor;
  private byte[] buffer;
  private final int bufferSize;
  private CompressionFileState compressionInfo;
  private CompressionCodec compressionCodec;

  private OutputStream out;
  private InputStream in;

  private long originPos = 0;
  private long compressedPos = 0;
  private List<Long> originPositions = new ArrayList<>();
  private List<Long> compressedPositions = new ArrayList<>();

  public SmartCompressorStream(InputStream inputStream, OutputStream outputStream,
      int bufferSize, CompressionFileState compressionInfo) {
    this.out = outputStream;
    this.in = inputStream;
    this.compressionInfo = compressionInfo;
    this.compressionCodec = new CompressionCodec();

    this.bufferSize = bufferSize;
    int overHead = bufferSize / 6 + 32;
    buffer = new byte[bufferSize + overHead];
    this.compressor = compressionCodec.createCompressor(bufferSize + overHead, compressionInfo.getCompressionImpl());
    if (compressor instanceof SnappyCompressor) {
      compressionInfo.setCompressionImpl("snappy");
    }
  }

  /**
   * Convert the original input stream to compressed output stream.
   */
  public void convert() throws IOException {
    byte[] buf = new byte[bufferSize];
    while (true) {
      int off = 0;
      // Compression trunk with trunk size of bufferSize
      while (off < bufferSize) {
        int len = in.read(buf, off, bufferSize - off);
        // Complete when input stream reaches eof
        if (len <= 0) {
          write(buf, 0, off);
          finish();
          out.close();
          compressionInfo.setPositionMapping(originPositions.toArray(new Long[0]),
              compressedPositions.toArray(new Long[0]));
          return;
        }
        off += len;
      }
      write(buf, 0, off);
      originPos += off;
    }
  }

  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) > b.length)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    originPositions.add(originPos);
    compressedPositions.add(compressedPos);

    compressor.setInput(b, off, len);
    compressor.finish();
    while (!compressor.finished()) {
      compress();
    }
    compressor.reset();
  }

  public void finish() throws IOException {
    if (!compressor.finished()) {
      compressor.finish();
      while (!compressor.finished()) {
        compress();
      }
    }
  }

  protected void compress() throws IOException {
    // TODO when compressed result is larger than raw
    int len = compressor.compress(buffer, 0, buffer.length);
    if (len > 0) {
      // Write out the compressed chunk
      rawWriteInt(len);
      out.write(buffer, 0, len);
      compressedPos += len;
    }
  }

  private void rawWriteInt(int v) throws IOException {
    out.write((v >>> 24) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>>  8) & 0xFF);
    out.write((v >>>  0) & 0xFF);
    compressedPos += 4;
  }
}
