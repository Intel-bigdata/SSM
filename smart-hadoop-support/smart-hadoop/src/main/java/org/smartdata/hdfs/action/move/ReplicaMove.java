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
package org.smartdata.hdfs.action.move;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.balancer.KeyManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.hdfs.CompatibilityHelperLoader;

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
      LOG.debug("Successfully moved " + this);
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
  private void sendRequest(
      DataOutputStream out, ExtendedBlock eb, Token<BlockTokenIdentifier> accessToken)
      throws IOException {
    CompatibilityHelperLoader.getHelper()
        .replaceBlock(
            out,
            eb,
            target.getStorageType(),
            accessToken,
            source.getDatanodeInfo().getDatanodeUuid(),
            source.getDatanodeInfo());
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
    checkBlockOpStatus(response, logInfo);
  }

  public static void checkBlockOpStatus(
    DataTransferProtos.BlockOpResponseProto response,
    String logInfo) throws IOException {
    if (response.getStatus() != DataTransferProtos.Status.SUCCESS) {
      if (response.getStatus() == DataTransferProtos.Status.ERROR_ACCESS_TOKEN) {
        throw new InvalidBlockTokenException(
          "Got access token error"
            + ", status message " + response.getMessage()
            + ", " + logInfo
        );
      } else {
        throw new IOException(
          "Got error"
            + ", status message " + response.getMessage()
            + ", " + logInfo
        );
      }
    }
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
   *
   * @param allMoves
   * @param ret ret[0] = number finished, ret[1] = number succeeded
   */
  public static void countStatus(List<ReplicaMove> allMoves, int[] ret) {
    ret[0] = 0;
    ret[1] = 0;
    for (ReplicaMove move : allMoves) {
      if (move.status.isFinished()) {
        ret[0] += 1;
        if (move.status.isSuccessful()) {
          ret[1] += 1;
        }
      }
    }
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
