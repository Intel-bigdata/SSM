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

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.htrace.TraceScope;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class SmartDFSInputStream extends DFSInputStream {
  private DFSClient dfsClient;
  private String src;
  boolean verifyChecksum;
  private byte[] oneByteBuf; // used for 'int read()'
  private final ReadStatistics readStatistics = new ReadStatistics();
  private final Object infoLock = new Object();
  private AtomicBoolean closed = new AtomicBoolean(false);
  private int failures = 0;

  // state by stateful read only:
  // (protected by lock on this)
  /////
  private DatanodeInfo currentNode = null;
  private LocatedBlock currentLocatedBlock = null;
  private long pos = 0;
  private long blockEnd = -1;
  private BlockReader blockReader = null;
  ////

  // state shared by stateful and positional read:
  // (protected by lock on infoLock)
  ////
  private LocatedBlocks locatedBlocks = null;
  private long lastBlockBeingWrittenLength = 0;
  private FileEncryptionInfo fileEncryptionInfo = null;
  private CachingStrategy cachingStrategy;

  public SmartDFSInputStream(DFSClient dfsClient, String src,
      boolean verifyChecksum) throws IOException {
    super(dfsClient, src, verifyChecksum);
    this.dfsClient = dfsClient;
    this.src = src;
    this.verifyChecksum = verifyChecksum;
  }

  @Override
  void openInfo() throws IOException, UnresolvedLinkException {
  }

  void openInfo2() throws IOException, UnresolvedLinkException {
  }

  @Override
  public synchronized int read() throws IOException {
    if (oneByteBuf == null) {
      oneByteBuf = new byte[1];
    }
    int ret = read( oneByteBuf, 0, 1 );
    return ( ret <= 0 ) ? -1 : (oneByteBuf[0] & 0xff);
  }

  @Override
  public synchronized int read(final byte buf[], int off, int len) throws IOException {
    ReaderStrategy byteArrayReader = new ByteArrayStrategy(buf);
    TraceScope scope =
        dfsClient.getPathTraceScope("DFSInputStream#byteArrayRead", src);
    try {
      return readWithStrategy(byteArrayReader, off, len);
    } finally {
      scope.close();
    }
  }

  @Override
  public synchronized int read(final ByteBuffer buf) throws IOException {
    ReaderStrategy byteBufferReader = new ByteBufferStrategy(buf);
    TraceScope scope =
        dfsClient.getPathTraceScope("DFSInputStream#byteBufferRead", src);
    try {
      return readWithStrategy(byteBufferReader, 0, buf.remaining());
    } finally {
      scope.close();
    }
  }

  /** This is a used by regular read() and handles ChecksumExceptions.
      * name readBuffer() is chosen to imply similarity to readBuffer() in
  * ChecksumFileSystem
  */
  private synchronized int readBuffer(ReaderStrategy reader, int off, int len,
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    IOException ioe;

    /* we retry current node only once. So this is set to true only here.
     * Intention is to handle one common case of an error that is not a
     * failure on datanode or client : when DataNode closes the connection
     * since client is idle. If there are other cases of "non-errors" then
     * then a datanode might be retried by setting this to true again.
     */
    boolean retryCurrentNode = true;

    while (true) {
      // retry as many times as seekToNewSource allows.
      try {
        return reader.doRead(blockReader, off, len);
      } catch ( ChecksumException ce ) {
        DFSClient.LOG.warn("Found Checksum error for "
            + getCurrentBlock() + " from " + currentNode
            + " at " + ce.getPos());
        ioe = ce;
        retryCurrentNode = false;
        // we want to remember which block replicas we have tried
        addIntoCorruptedBlockMap(getCurrentBlock(), currentNode,
            corruptedBlockMap);
      } catch ( IOException e ) {
        if (!retryCurrentNode) {
          DFSClient.LOG.warn("Exception while reading from "
              + getCurrentBlock() + " of " + src + " from "
              + currentNode, e);
        }
        ioe = e;
      }
      boolean sourceFound = false;
      if (retryCurrentNode) {
        /* possibly retry the same node so that transient errors don't
         * result in application level failures (e.g. Datanode could have
         * closed the connection because the client is idle for too long).
         */
        sourceFound = seekToBlockSource(pos);
      } else {
        addToDeadNodes(currentNode);
        sourceFound = seekToNewSource(pos);
      }
      if (!sourceFound) {
        throw ioe;
      }
      retryCurrentNode = false;
    }
  }

  private synchronized int readWithStrategy(ReaderStrategy strategy, int off, int len) throws IOException {
    dfsClient.checkOpen();
    if (closed.get()) {
      throw new IOException("Stream closed");
    }
    Map<ExtendedBlock,Set<DatanodeInfo>> corruptedBlockMap
        = new HashMap<ExtendedBlock, Set<DatanodeInfo>>();
    failures = 0;
    if (pos < getFileLength()) {
      int retries = 2;
      while (retries > 0) {
        try {
          // currentNode can be left as null if previous read had a checksum
          // error on the same block. See HDFS-3067
          if (pos > blockEnd || currentNode == null) {
            currentNode = blockSeekTo(pos);
          }
          int realLen = (int) Math.min(len, (blockEnd - pos + 1L));
          synchronized(infoLock) {
            if (locatedBlocks.isLastBlockComplete()) {
              realLen = (int) Math.min(realLen,
                  locatedBlocks.getFileLength() - pos);
            }
          }
          int result = readBuffer(strategy, off, realLen, corruptedBlockMap);

          if (result >= 0) {
            pos += result;
          } else {
            // got a EOS from reader though we expect more data on it.
            throw new IOException("Unexpected EOS from the reader");
          }
          if (dfsClient.stats != null) {
            dfsClient.stats.incrementBytesRead(result);
          }
          return result;
        } catch (ChecksumException ce) {
          throw ce;
        } catch (IOException e) {
          if (retries == 1) {
            DFSClient.LOG.warn("DFS Read", e);
          }
          blockEnd = -1;
          if (currentNode != null) { addToDeadNodes(currentNode); }
          if (--retries == 0) {
            throw e;
          }
        } finally {
          // Check if need to report block replicas corruption either read
          // was successful or ChecksumException occured.
          reportCheckSumFailure(corruptedBlockMap,
              currentLocatedBlock.getLocations().length);
        }
      }
    }
    return -1;
  }

  /**
   * Wraps different possible read implementations so that readBuffer can be
   * strategy-agnostic.
   */
  private interface ReaderStrategy {
    public int doRead(BlockReader blockReader, int off, int len)
        throws ChecksumException, IOException;
  }

  /**
   * Used to read bytes into a byte[]
   */
  private class ByteArrayStrategy implements ReaderStrategy {
    final byte[] buf;

    public ByteArrayStrategy(byte[] buf) {
      this.buf = buf;
    }

    @Override
    public int doRead(BlockReader blockReader, int off, int len)
        throws ChecksumException, IOException {
      int nRead = blockReader.read(buf, off, len);
      updateReadStatistics(readStatistics, nRead, blockReader);
      return nRead;
    }
  }

  /**
   * Used to read bytes into a user-supplied ByteBuffer
   */
  private class ByteBufferStrategy implements ReaderStrategy {
    final ByteBuffer buf;
    ByteBufferStrategy(ByteBuffer buf) {
      this.buf = buf;
    }

    @Override
    public int doRead(BlockReader blockReader, int off, int len)
        throws ChecksumException, IOException {
      int oldpos = buf.position();
      int oldlimit = buf.limit();
      boolean success = false;
      try {
        int ret = blockReader.read(buf);
        success = true;
        updateReadStatistics(readStatistics, ret, blockReader);
        return ret;
      } finally {
        if (!success) {
          // Reset to original state so that retries work correctly.
          buf.position(oldpos);
          buf.limit(oldlimit);
        }
      }
    }
  }

  private void updateReadStatistics(ReadStatistics readStatistics,
      int nRead, BlockReader blockReader) {
    if (nRead <= 0) return;
    synchronized(infoLock) {
      if (blockReader.isShortCircuit()) {
        readStatistics.addShortCircuitBytes(nRead);
      } else if (blockReader.isLocal()) {
        readStatistics.addLocalBytes(nRead);
      } else {
        readStatistics.addRemoteBytes(nRead);
      }
    }
  }

//  @Override
//  public synchronized ByteBuffer read(ByteBufferPool bufferPool,
//      int maxLength, EnumSet<ReadOption> opts)
//      throws IOException, UnsupportedOperationException {
//    if (maxLength == 0) {
//      return EMPTY_BUFFER;
//    } else if (maxLength < 0) {
//      throw new IllegalArgumentException("can't read a negative " +
//          "number of bytes.");
//    }
//    if ((blockReader == null) || (blockEnd == -1)) {
//      if (pos >= getFileLength()) {
//        return null;
//      }
//      /*
//       * If we don't have a blockReader, or the one we have has no more bytes
//       * left to read, we call seekToBlockSource to get a new blockReader and
//       * recalculate blockEnd.  Note that we assume we're not at EOF here
//       * (we check this above).
//       */
//      if ((!seekToBlockSource(pos)) || (blockReader == null)) {
//        throw new IOException("failed to allocate new BlockReader " +
//            "at position " + pos);
//      }
//    }
//    ByteBuffer buffer = null;
//    if (dfsClient.getConf().shortCircuitMmapEnabled) {
//      buffer = tryReadZeroCopy(maxLength, opts);
//    }
//    if (buffer != null) {
//      return buffer;
//    }
//    buffer = ByteBufferUtil.fallbackRead(this, bufferPool, maxLength);
//    if (buffer != null) {
//      getExtendedReadBuffers().put(buffer, bufferPool);
//    }
//    return buffer;
//  }
}
