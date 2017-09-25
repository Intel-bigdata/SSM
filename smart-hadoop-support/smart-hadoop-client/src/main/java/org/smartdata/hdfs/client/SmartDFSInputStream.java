package org.smartdata.hdfs.client;

import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;

import java.io.IOException;

public class SmartDFSInputStream extends DFSInputStream {
  private DFSClient dfsClient;

  public SmartDFSInputStream(DFSClient dfsClient, String src, boolean verifyChecksum)
      throws IOException, UnresolvedLinkException {
    try {
      super(dfsClient, src, verifyChecksum);
    } catch (Exception e) {

    }
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

  @Override
  public synchronized ByteBuffer read(ByteBufferPool bufferPool,
      int maxLength, EnumSet<ReadOption> opts)
      throws IOException, UnsupportedOperationException {
    if (maxLength == 0) {
      return EMPTY_BUFFER;
    } else if (maxLength < 0) {
      throw new IllegalArgumentException("can't read a negative " +
          "number of bytes.");
    }
    if ((blockReader == null) || (blockEnd == -1)) {
      if (pos >= getFileLength()) {
        return null;
      }
      /*
       * If we don't have a blockReader, or the one we have has no more bytes
       * left to read, we call seekToBlockSource to get a new blockReader and
       * recalculate blockEnd.  Note that we assume we're not at EOF here
       * (we check this above).
       */
      if ((!seekToBlockSource(pos)) || (blockReader == null)) {
        throw new IOException("failed to allocate new BlockReader " +
            "at position " + pos);
      }
    }
    ByteBuffer buffer = null;
    if (dfsClient.getConf().shortCircuitMmapEnabled) {
      buffer = tryReadZeroCopy(maxLength, opts);
    }
    if (buffer != null) {
      return buffer;
    }
    buffer = ByteBufferUtil.fallbackRead(this, bufferPool, maxLength);
    if (buffer != null) {
      getExtendedReadBuffers().put(buffer, bufferPool);
    }
    return buffer;
  }
}
