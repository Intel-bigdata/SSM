package org.smartdata.hdfs.action.move;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.balancer.KeyManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Iterator;
import java.util.List;

/**
 * One single move represents the move action of one replication.
 */
class ReplicaMove {
  static final Logger LOG = LoggerFactory.getLogger(ReplicaMove.class);

  private NameNodeConnector nnc;
  private SaslDataTransferClient saslClient;

  private Block block;
  private StorageGroup target;
  private StorageGroup source;
  private ReplicaMoveStatus status;

  public ReplicaMove(Block block, StorageGroup source, StorageGroup target,
                     NameNodeConnector nnc, SaslDataTransferClient saslClient) {
    this.nnc = nnc;
    this.saslClient = saslClient;
    this.block = block;
    this.target = target;
    this.source = source;
    this.status = new ReplicaMoveStatus();
  }

  @Override
  public String toString() {
    String bStr = block != null ? (block + " with size=" + block.getNumBytes() + " ")
        : " ";
    return bStr + "from " + source.getDisplayName() + " to " + target.getDisplayName();
  }

  public void run() {
    LOG.debug("Start moving " + this);

    Socket sock = new Socket();
    DataOutputStream out = null;
    DataInputStream in = null;
    try {
      sock.connect(
          NetUtils.createSocketAddr(target.getDatanodeInfo().getXferAddr()),
          HdfsServerConstants.READ_TIMEOUT);

      sock.setKeepAlive(true);

      OutputStream unbufOut = sock.getOutputStream();
      InputStream unbufIn = sock.getInputStream();
      ExtendedBlock eb = new ExtendedBlock(nnc.getBlockpoolID(), block);
      final KeyManager km = nnc.getKeyManager();
      Token<BlockTokenIdentifier> accessToken = km.getAccessToken(eb);
      IOStreamPair saslStreams = saslClient.socketSend(sock, unbufOut,
          unbufIn, km, accessToken, target.getDatanodeInfo());
      unbufOut = saslStreams.out;
      unbufIn = saslStreams.in;
      out = new DataOutputStream(new BufferedOutputStream(unbufOut,
          HdfsConstants.IO_FILE_BUFFER_SIZE));
      in = new DataInputStream(new BufferedInputStream(unbufIn,
          HdfsConstants.IO_FILE_BUFFER_SIZE));

      sendRequest(out, eb, accessToken);
      receiveResponse(in);
      LOG.info("Successfully moved " + this);
      status.setSuccessful(true);
    } catch (IOException e) {
      LOG.warn("Failed to move " + this + ": " + e.getMessage());
      status.setSuccessful(false);
    } finally {
      IOUtils.closeStream(out);
      IOUtils.closeStream(in);
      IOUtils.closeSocket(sock);
      status.setFinished(true);
    }
  }

  /** Send a block replace request to the output stream */
  private void sendRequest(DataOutputStream out, ExtendedBlock eb,
      Token<BlockTokenIdentifier> accessToken) throws IOException {
    new Sender(out).replaceBlock(eb, target.getStorageType(), accessToken,
        source.getDatanodeInfo().getDatanodeUuid(), source.getDatanodeInfo());
  }

  /** Receive a block copy response from the input stream */
  private void receiveResponse(DataInputStream in) throws IOException {
    DataTransferProtos.BlockOpResponseProto response =
        DataTransferProtos.BlockOpResponseProto.parseFrom(PBHelper.vintPrefixed(in));
    while (response.getStatus() == DataTransferProtos.Status.IN_PROGRESS) {
      // read intermediate responses
      response = DataTransferProtos.BlockOpResponseProto.parseFrom(PBHelper.vintPrefixed(in));
    }
    String logInfo = "block move is failed";
    DataTransferProtoUtil.checkBlockOpStatus(response, logInfo);
  }

  public static int failedMoves(List<ReplicaMove> allMoves) {
    int failedNum = 0;
    for (ReplicaMove move : allMoves) {
      if (!move.status.isSuccessful()) {
        failedNum += 1;
      }
    }
    return failedNum;
  }

  public static boolean allMoveFinished(List<ReplicaMove> allMoves) {
    for (ReplicaMove move : allMoves) {
      if (!move.status.isFinished()){
        return false;
      }
    }
    return true;
  }

  /**
   * Remove successful moves and refresh the status of remaining ones for a new iteration.
   * @param allMoves
   * @return number of remaining moves
   */
  public static int refreshMoverList(List<ReplicaMove> allMoves) {
    for (Iterator<ReplicaMove> it = allMoves.iterator(); it.hasNext();) {
      ReplicaMove replicaMove = it.next();
      if (replicaMove.status.isSuccessful()) {
        it.remove();
      } else {
        replicaMove.status.reset();
      }
    }
    return allMoves.size();
  }

  /**
   * A class for tracking the status of a single move.
   */
  class ReplicaMoveStatus {
    private boolean finished;
    private boolean successful;

    public ReplicaMoveStatus() {
      finished = false;
      successful = false;
    }

    public void setFinished(boolean finished) {
      this.finished = finished;
    }

    public void setSuccessful(boolean successful) {
      this.successful = successful;
    }

    public boolean isFinished() {
      return finished;
    }

    public boolean isSuccessful() {
      return successful;
    }

    public void reset() {
      finished = false;
      successful = false;
    }
  }
}
