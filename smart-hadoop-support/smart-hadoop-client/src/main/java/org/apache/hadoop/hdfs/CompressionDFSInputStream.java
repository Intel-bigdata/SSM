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

import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.smartdata.model.CompressionTrunk;
import org.smartdata.model.SmartFileCompressionInfo;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

/**
 * CompressionDFSInputStream.
 */
public class CompressionDFSInputStream extends DFSInputStream {

  private Decompressor decompressor = null;
  private byte[] buffer;
  private boolean closed = false;
  private long pos = 0;

  private SmartFileCompressionInfo compressionInfo;
  private final long originalLength;

  public CompressionDFSInputStream(DFSClient dfsClient, String src, boolean verifyChecksum,
      SmartFileCompressionInfo compressionInfo) throws IOException,
      UnresolvedLinkException {
    super(dfsClient, src, verifyChecksum);
    this.compressionInfo = compressionInfo;
    originalLength = compressionInfo.getOriginalLength();
    int bufferSize = compressionInfo.getBufferSize();
    this.decompressor = new SnappyDecompressor(bufferSize);
    this.buffer = new byte[bufferSize];
  }

  @Override
  public synchronized int read() throws IOException {
    byte[] oneByteBuf = new byte[1];
    int ret = read( oneByteBuf, 0, 1 );
    return ( ret <= 0 ) ? -1 : (oneByteBuf[0] & 0xff);
  }

  @Override
  public synchronized int read(final byte b[], int off, int len) throws IOException {
    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    int n = decompress(b, off, len);
    pos += n;
    return n;
  }

  @Override
  public synchronized int read(final ByteBuffer buf) throws IOException {
    return read(buf.array(), buf.position(), buf.remaining());
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    seek(position);
    return read(buffer, offset, length);
  }

  @Override
  public synchronized void close() throws IOException {
    super.close();
    this.closed = true;
  }

  private int decompress(byte[] b, int off, int len) throws IOException {
    int n = 0;
    while ((n = decompressor.decompress(b, off, len)) == 0) {
      if (decompressor.needsInput()) {
        int m;
        try {
          m = getCompressedData();
        } catch (EOFException e) {
          return -1;
        }
        // Send the read data to the decompressor
        decompressor.setInput(buffer, 0, m);
      }
    }

    // Note the no. of decompressed bytes read from 'current' block
    //noUncompressedBytes += n;

    return n;
  }

  private int getCompressedData() throws IOException {
    // Get the size of the compressed chunk (always non-negative)
    int len = rawReadInt();

    // Read len bytes from underlying stream
    if (len > buffer.length) {
      buffer = new byte[len];
    }
    int n = 0, off = 0;
    while (n < len) {
      int count = super.read(buffer, off + n, len - n);
      if (count < 0) {
        throw new EOFException("Unexpected end of block in input stream");
      }
      n += count;
    }

    return len;
  }

  private int rawReadInt() throws IOException {
    byte[] bytes = new byte[4];
    int b1 = super.read(bytes, 0, 1);
    int b2 = super.read(bytes, 1, 1);
    int b3 = super.read(bytes, 2, 1);
    int b4 = super.read(bytes, 3, 1);
    if ((b1 | b2 | b3 | b4) < 0)
      throw new EOFException();
    return (((bytes[0] & 0xff) << 24) + ((bytes[1] & 0xff) << 16)
        + ((bytes[2] & 0xff) << 8) + ((bytes[3] & 0xff) << 0));
  }

/*
  @Override
  public long getFileLength() {
    return compressionInfo.getOriginalLength();
  }
  */

  @Override
  public long skip(long n) throws IOException {
    return super.skip(n);
  }

  @Override
  public synchronized void seek(long targetPos) throws IOException {
    if (targetPos > originalLength) {
      throw new EOFException("Cannot seek after EOF");
    }
    if (targetPos < 0) {
      throw new EOFException("Cannot seek to negative offset");
    }
    if(targetPos == pos) {
      return;
    }

    // Seek to the start of the compression trunk
    CompressionTrunk compressionTrunk = compressionInfo.locateCompressionTrunk(
        false, targetPos);
    long hdfsFilePos = compressionTrunk.getCompressedOffset();
    super.seek(hdfsFilePos);

    // Decompress the trunk until reaching the targetPos of the original file
    int startPos = (int)(targetPos - compressionTrunk.getOriginOffset());
    decompressor.reset();
    int m = getCompressedData();
    decompressor.setInput(buffer, 0, m);
    if (startPos > 0) {
      byte[] temp = new byte[startPos];
      decompress(temp, 0, startPos);
    }
    pos = targetPos;
  }

  @Override
  public synchronized boolean seekToNewSource(long targetPos) throws IOException {
    throw new RuntimeException("SeekToNewSource not supported for compressed file");
  }

  @Override
  public synchronized long getPos() throws IOException {
    return pos;
  }

  @Override
  public synchronized int available() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    final long remaining = originalLength - pos;
    return remaining <= Integer.MAX_VALUE? (int)remaining: Integer.MAX_VALUE;
  }

  @Override
  public ReadStatistics getReadStatistics() {
    throw new RuntimeException("GetReadStatistics not supported for compressed file");
  }

  @Override
  public void clearReadStatistics() {
    throw new RuntimeException("ClearReadStatistics not supported for compressed file");
  }

  @Override
  public FileEncryptionInfo getFileEncryptionInfo() {
    throw new RuntimeException("GetFileEncryptionInfo not supported for compressed file");
  }

  @Override
  public synchronized ByteBuffer read(ByteBufferPool bufferPool,
      int maxLength, EnumSet<ReadOption> opts)
      throws IOException, UnsupportedOperationException {
    throw new RuntimeException("Read(ByteBufferPool, int, EnumSet) not supported " +
        "for compressed file");
  }
}
