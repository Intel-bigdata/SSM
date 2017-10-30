package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.io.compress.BlockCompressorStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.smartdata.conf.SmartConfKeys;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.ClosedChannelException;
import java.util.EnumSet;

/**
 * SmartOutputStream.
 */
public class SmartDFSOutputStream extends FilterOutputStream {

  private Compressor compressor;
  private BlockCompressorStream blockCompressorStream;

  private final int bufferSize;
  private final int compressionOverhead;

  private volatile boolean closed;

  private SmartDFSOutputStream(DFSOutputStream dfsOutputStream,
      Configuration conf) {
    super(dfsOutputStream);
    this.closed = false;
    String compressionImpl = conf.get(SmartConfKeys.SMART_COMPRESSION_IMPL,
        SmartConfKeys.SMART_COMPRESSION_IMPL_DEFAULT);
    if (compressionImpl.equals("snappy")) {
      compressor = new SnappyCompressor();
    } else {
      throw new RuntimeException("Unsupported compressor: " + compressionImpl);
    }
    bufferSize = conf.getInt(SmartConfKeys.SMART_COMPRESSION_BUFFER_SIZE,
        SmartConfKeys.SMART_COMPRESSION_BUFFER_SIZE_DEFAULT);
    compressionOverhead = (bufferSize / 6) + 32;
    blockCompressorStream = new BlockCompressorStream(out, compressor,
        bufferSize, compressionOverhead);
  }

  static public SmartDFSOutputStream newStreamForCreate(DFSClient dfsClient,
      String src, FsPermission masked, EnumSet<CreateFlag> flag,
      boolean createParent, short replication, long blockSize,
      Progressable progress, int buffersize, DataChecksum checksum,
      String[] favoredNodes, Configuration conf) throws IOException {
    DFSOutputStream dfsOutputStream = DFSOutputStream.newStreamForCreate(
        dfsClient, src, masked, flag, createParent, replication, blockSize,
        progress, buffersize, checksum, favoredNodes);
    SmartDFSOutputStream smartDFSOutputStream = new SmartDFSOutputStream(
        dfsOutputStream, conf);
    return smartDFSOutputStream;
  }

  @Override
  public void write(int b) throws IOException {
    byte[] buf = new byte[]{(byte)b};
    write(buf, 0, 1);
  }

  @Override
  public void write(byte b[], int off, int len) throws IOException {
    blockCompressorStream.write(b, off, len);
  }

  @Override
  public void close() throws IOException {
    blockCompressorStream.close();
    closed = true;
  }

  private void checkClosed() throws IOException {
    if (closed) {
      throw new ClosedChannelException();
    }
  }
}
