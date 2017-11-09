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
import org.smartdata.model.FileContainerInfo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class SmartDFSInputStream extends DFSInputStream {
  private FileContainerInfo containerFileInfo;

  public SmartDFSInputStream(DFSClient dfsClient, String containerFile,
      boolean verifyChecksum, FileContainerInfo containerFileInfo) throws IOException {
    super(dfsClient, containerFile, verifyChecksum);
    this.containerFileInfo = containerFileInfo;
    super.seek(containerFileInfo.getOffset());
  }

  @Override
  public long getFileLength() {
    String callerClass = Thread.currentThread().getStackTrace()[2].getClassName();
    if (callerClass.equals("org.apache.hadoop.hdfs.DFSInputStream")) {
      return containerFileInfo.getLength() + containerFileInfo.getOffset();
    }
    return containerFileInfo.getLength();
  }

  @Override
  public List<LocatedBlock> getAllBlocks() throws IOException {
    List<LocatedBlock> blocks = super.getAllBlocks();
    List<LocatedBlock> ret = new ArrayList<>();
    long off = containerFileInfo.getOffset();
    long len = containerFileInfo.getLength();
    for (LocatedBlock b : blocks) {
      if (off > b.getStartOffset() + b.getBlockSize() || off + len < b.getStartOffset()) {
        continue;
      }
      ret.add(b);
    }
    return ret;
  }

  @Override
  public synchronized int read(final byte buf[], int off, int len) throws IOException {
    int realLen = (int) Math.min(len, containerFileInfo.getLength());
    return super.read(buf, off, realLen);
  }

  @Override
  public synchronized int read(final ByteBuffer buf) throws IOException {
    int realLen = (int) Math.min(buf.remaining(), containerFileInfo.getLength());
    buf.limit(realLen + buf.position());
    return super.read(buf);
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    int realLen = (int) Math.min(length, containerFileInfo.getLength());
    long realPos = position + containerFileInfo.getOffset();
    return super.read(realPos, buffer, offset, realLen);
  }

  @Override
  public synchronized long getPos() throws IOException {
    return super.getPos() - containerFileInfo.getOffset();
  }

  @Override
  public synchronized void setReadahead(Long readahead) throws IOException {
    long realReadAhead = Math.min(readahead, containerFileInfo.getLength());
    super.setReadahead(realReadAhead);
  }

  @Override
  public synchronized ByteBuffer read(ByteBufferPool bufferPool,
                                      int maxLength, EnumSet<ReadOption> opts)
          throws IOException, UnsupportedOperationException {
    int realMaxLen = (int) Math.min(maxLength, containerFileInfo.getLength());
    return super.read(bufferPool, realMaxLen, opts);
  }
}
