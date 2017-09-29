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

import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.smartdata.model.FileContainerInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SmartDFSInputStream extends DFSInputStream {
  private FileContainerInfo containerFileInfo;
  private final DFSClient dfsClient;
  private AtomicBoolean closed = new AtomicBoolean(false);
  private final String src;
  private final boolean verifyChecksum;

  public SmartDFSInputStream(DFSClient dfsClient, String src,
      boolean verifyChecksum, FileContainerInfo containerFileInfo) throws IOException {
    super(dfsClient, containerFileInfo.getContainerFilePath(), verifyChecksum);
    super.seek(containerFileInfo.getOffset());
    this.dfsClient = dfsClient;
    this.src = src;
    this.verifyChecksum = verifyChecksum;
    this.containerFileInfo = containerFileInfo;
  }


//  @Override
//  public synchronized int read() throws IOException {
//  }
//
//  @Override
//  public synchronized int read(final byte buf[], int off, int len) throws IOException {
//  }
//
//  @Override
//  public synchronized int read(final ByteBuffer buf) throws IOException {
//  }
//
//  @Override
//  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
//  }
//
//  @Override
//  public synchronized ByteBuffer read(ByteBufferPool bufferPool,
//      int maxLength, EnumSet<ReadOption> opts)
//      throws IOException, UnsupportedOperationException {
//  }




  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    return super.read(position + containerFileInfo.getOffset(), buffer, offset, length);
  }

  @Override
  public long getFileLength() {
    return containerFileInfo.getLength();
  }

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

  // TODO: check if right or not
//  /**
//   * Seek to given position on a node other than the current node.  If
//   * a node other than the current node is found, then returns true.
//   * If another node could not be found, then returns false.
//   */
//  @Override
//  public synchronized boolean seekToNewSource(long targetPos) throws IOException {
//  }

  /**
   */
  @Override
  public synchronized long getPos() throws IOException {
    return super.getPos() - containerFileInfo.getOffset();
  }

  /** Return the size of the remaining available bytes
   * if the size is less than or equal to {@link Integer#MAX_VALUE},
   * otherwise, return {@link Integer#MAX_VALUE}.
   */
  @Override
  public synchronized int available() throws IOException {
    if (closed.get()) {
      throw new IOException("Stream closed");
    }

    final long remaining = getFileLength() - getPos();
    return remaining <= Integer.MAX_VALUE? (int)remaining: Integer.MAX_VALUE;
  }

//  @Override
//  public synchronized void setReadahead(Long readahead)
//  }
//
//  @Override
//  public synchronized void setDropBehind(Boolean dropBehind)
//  }
}
