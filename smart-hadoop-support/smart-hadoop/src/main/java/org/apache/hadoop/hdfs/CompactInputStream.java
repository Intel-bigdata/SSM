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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class CompactInputStream extends SmartInputStream {
  private FileContainerInfo fileContainerInfo;

  CompactInputStream(DFSClient dfsClient, String src, boolean verifyChecksum,
                     FileState fileState) throws IOException {
    super(dfsClient, ((CompactFileState) fileState).getFileContainerInfo().getContainerFilePath(),
        verifyChecksum, fileState);
    this.fileContainerInfo = ((CompactFileState) fileState).getFileContainerInfo();
    super.seek(fileContainerInfo.getOffset());
  }

  @Override
  public long getFileLength() {
    String callerClass = Thread.currentThread().getStackTrace()[2].getClassName();
    if (callerClass.equals("org.apache.hadoop.hdfs.DFSInputStream")) {
      return fileContainerInfo.getLength() + fileContainerInfo.getOffset();
    }
    return fileContainerInfo.getLength();
  }

  @Override
  public List<LocatedBlock> getAllBlocks() throws IOException {
    List<LocatedBlock> blocks = super.getAllBlocks();
    List<LocatedBlock> ret = new ArrayList<>();
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
  public synchronized int read(final byte buf[], int off, int len) throws IOException {
    int realLen = (int) Math.min(len, fileContainerInfo.getLength());
    return super.read(buf, off, realLen);
  }

  @Override
  public synchronized int read(final ByteBuffer buf) throws IOException {
    int realLen = (int) Math.min(buf.remaining(), fileContainerInfo.getLength());
    buf.limit(realLen + buf.position());
    return super.read(buf);
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    int realLen = (int) Math.min(length, fileContainerInfo.getLength());
    long realPos = position + fileContainerInfo.getOffset();
    return super.read(realPos, buffer, offset, realLen);
  }

  @Override
  public synchronized long getPos() throws IOException {
    return super.getPos() - fileContainerInfo.getOffset();
  }

  @Override
  public synchronized void setReadahead(Long readahead) throws IOException {
    long realReadAhead = Math.min(readahead, fileContainerInfo.getLength());
    super.setReadahead(realReadAhead);
  }

  @Override
  public synchronized ByteBuffer read(ByteBufferPool bufferPool,
                                      int maxLength, EnumSet<ReadOption> opts)
      throws IOException, UnsupportedOperationException {
    int realMaxLen = (int) Math.min(maxLength, fileContainerInfo.getLength());
    return super.read(bufferPool, realMaxLen, opts);
  }
}
