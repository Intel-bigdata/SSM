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

import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.ByteBufferPool;
import org.smartdata.model.CompactFileState;
import org.smartdata.model.FileContainerInfo;
import org.smartdata.model.FileState;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class CompactInputStream extends SmartInputStream {
  private FileContainerInfo fileContainerInfo;
  private boolean closed = false;
  private static final String INHERITED_CLASS = "org.apache.hadoop.hdfs.DFSInputStream";

  CompactInputStream(DFSClient dfsClient, boolean verifyChecksum,
                     FileState fileState) throws IOException {
    super(dfsClient,
          ((CompactFileState) fileState).getFileContainerInfo().getContainerFilePath(),
          verifyChecksum,
          fileState);
    this.fileContainerInfo = ((CompactFileState) fileState).getFileContainerInfo();
    super.seek(fileContainerInfo.getOffset());
  }

  @Override
  public long getFileLength() {
    String callerClass = Thread.currentThread().getStackTrace()[2].getClassName();
    if (INHERITED_CLASS.equals(callerClass)) {
      return fileContainerInfo.getLength() + fileContainerInfo.getOffset();
    } else {
      return fileContainerInfo.getLength();
    }
  }

  @Override
  public List<LocatedBlock> getAllBlocks() throws IOException {
    List<LocatedBlock> blocks = super.getAllBlocks();
    List<LocatedBlock> ret = new ArrayList<>(16);
    long off = fileContainerInfo.getOffset();
    long len = fileContainerInfo.getLength();
    for (LocatedBlock b : blocks) {
      if (off > b.getStartOffset() + b.getBlockSize() || off + len < b.getStartOffset()) {
        continue;
      }
      ret.add(b);
    }
    return ret;
  }

  @Override
  public synchronized int read(final byte[] buf, int off, int len) throws IOException {
    int realLen = (int) Math.min(len, fileContainerInfo.getLength() - getPos());
    if (realLen == 0) {
      return -1;
    } else {
      return super.read(buf, off, realLen);
    }
  }

  @Override
  public synchronized int read(final ByteBuffer buf) throws IOException {
    int realLen = (int) Math.min(buf.remaining(), fileContainerInfo.getLength() - getPos());
    if (realLen == 0) {
      return -1;
    } else {
      buf.limit(realLen + buf.position());
      return super.read(buf);
    }
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    long realPos = position + fileContainerInfo.getOffset();
    int realLen = (int) Math.min(length, fileContainerInfo.getLength() - position);
    if (realLen == 0) {
      return -1;
    } else {
      return super.read(realPos, buffer, offset, realLen);
    }
  }

  @Override
  public synchronized long getPos() throws IOException {
    String callerClass = Thread.currentThread().getStackTrace()[2].getClassName();
    if (INHERITED_CLASS.equals(callerClass)) {
      return super.getPos();
    } else {
      return super.getPos() - fileContainerInfo.getOffset();
    }
  }

  @Override
  public synchronized int available() throws IOException {
    if (closed) {
      throw new IOException("Stream closed.");
    }
    final long remaining = getFileLength() - getPos();
    return remaining <= Integer.MAX_VALUE ? (int) remaining : Integer.MAX_VALUE;
  }

  @Override
  public synchronized void seek(long targetPos) throws IOException {
    String callerClass = Thread.currentThread().getStackTrace()[2].getClassName();
    if (INHERITED_CLASS.equals(callerClass)) {
      super.seek(targetPos);
    } else {
      if (targetPos > fileContainerInfo.getLength()) {
        throw new EOFException("Cannot seek after EOF");
      }
      if (targetPos < 0) {
        throw new EOFException("Cannot seek to negative offset");
      }
      super.seek(fileContainerInfo.getOffset() + targetPos);
    }
  }

  @Override
  public synchronized boolean seekToNewSource(long targetPos) throws IOException {
    String callerClass = Thread.currentThread().getStackTrace()[2].getClassName();
    if (INHERITED_CLASS.equals(callerClass)) {
      return super.seekToNewSource(targetPos);
    } else {
      if (targetPos < 0) {
        throw new EOFException("Cannot seek after EOF");
      } else {
        return super.seekToNewSource(fileContainerInfo.getOffset() + targetPos);
      }
    }
  }

  @Override
  public synchronized void setReadahead(Long readahead) throws IOException {
    long realReadAhead = Math.min(readahead, fileContainerInfo.getLength() - getPos());
    super.setReadahead(realReadAhead);
  }

  @Override
  public synchronized ByteBuffer read(ByteBufferPool bufferPool,
                                      int maxLength, EnumSet<ReadOption> opts)
      throws IOException, UnsupportedOperationException {
    int realMaxLen = (int) Math.min(maxLength, fileContainerInfo.getLength() - getPos());
    return super.read(bufferPool, realMaxLen, opts);
  }

  @Override
  public synchronized void close() throws IOException {
    super.close();
    this.closed = true;
  }
}
