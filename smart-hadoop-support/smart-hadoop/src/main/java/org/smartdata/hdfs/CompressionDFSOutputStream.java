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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.CompressorStream;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.smartdata.conf.SmartConfKeys;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * SmartOutputStream.
 */
public class CompressionDFSOutputStream extends CompressorStream {

  // buffer to save input and compress when it is full as a whole block
  private byte[] bufferIn;
  private final int bufferSize;
  private int count;

  private volatile boolean closed = false;

  private int originPos = 0;
  private int compressedPos = 0;
  public List<Integer> originPositions;
  public List<Integer> compressedPositions;

  // The 'maximum' size of input data to be compressed, to account
  // for the overhead of the compression algorithm.
  private final int MAX_INPUT_SIZE;


  public CompressionDFSOutputStream(OutputStream outputStream, Compressor compressor,
      int bufferSize) {
    super(outputStream, compressor, bufferSize);
    int compressionOverhead = (bufferSize / 6) + 32;
    MAX_INPUT_SIZE = bufferSize - compressionOverhead;
    this.bufferSize = bufferSize;
    bufferIn = new byte[bufferSize];
    this.count = 0;
    originPositions = new ArrayList<>();
    compressedPositions = new ArrayList<>();
  }

  /**
   * Write the data provided to the compression codec, compressing no more
   * than the buffer size less the compression overhead as specified during
   * construction for each block.
   *
   * Each block contains the uncompressed length for the block, followed by
   * one or more length-prefixed blocks of compressed data.
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    // Sanity checks
    if (compressor.finished()) {
      throw new IOException("write beyond end of stream");
    }
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) > b.length)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    long limlen = compressor.getBytesRead();
    if (len + limlen > MAX_INPUT_SIZE && limlen > 0) {
      // Adding this segment would exceed the maximum size.
      // Flush data if we have it.
      finish();
      compressor.reset();
    }

    if (len > MAX_INPUT_SIZE) {
      // The data we're given exceeds the maximum size. Any data
      // we had have been flushed, so we write out this chunk in segments
      // not exceeding the maximum size until it is exhausted.
      rawWriteInt(len);
      do {
        int bufLen = Math.min(len, MAX_INPUT_SIZE);

        compressor.setInput(b, off, bufLen);
        compressor.finish();
        while (!compressor.finished()) {
          compress();
        }
        compressor.reset();
        off += bufLen;
        len -= bufLen;
      } while (len > 0);
      compressedPositions.add(compressedPos);
      return;
    }

    // Give data to the compressor
    compressor.setInput(b, off, len);
    if (!compressor.needsInput()) {
      // compressor buffer size might be smaller than the maximum
      // size, so we permit it to flush if required.
      rawWriteInt((int)compressor.getBytesRead());
      do {
        compress();
      } while (!compressor.needsInput());
    }
  }

  @Override
  public void finish() throws IOException {
    if (!compressor.finished()) {
      rawWriteInt((int)compressor.getBytesRead());
      compressor.finish();
      while (!compressor.finished()) {
        compress();
      }
    }
  }

  @Override
  protected void compress() throws IOException {
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

  @Override
  public void close() throws IOException {

    closed = true;
  }

  private void checkClosed() throws IOException {
    if (closed) {
      throw new ClosedChannelException();
    }
  }
}
