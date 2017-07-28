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
package org.smartdata.actions.hdfs.move;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.balancer.KeyManager;
import org.apache.hadoop.hdfs.server.balancer.MovedBlocks;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed;

/** Dispatching block replica moves between datanodes. */
@InterfaceAudience.Private
// TODO: this class will be abandoned, and some logic and inner class shall be refactored
// to outer class
public class Dispatcher {
  static final Log LOG = LogFactory.getLog(Dispatcher.class);

  /**
   * the period of time to delay the usage of a DataNode after hitting
   * errors when using it for migrating data
   */
  private static long delayAfterErrors = 10 * 1000;

  private final NameNodeConnector nnc;
  private final SaslDataTransferClient saslClient;

  private MovedBlocks<StorageGroup> movedBlocks;
  private final long movedWinWidth;

  private NetworkTopology cluster;

  private final ExecutorService moveExecutor;
  private final ExecutorService dispatchExecutor;

  /** The maximum number of concurrent blocks moves at a datanode */
  private final int maxConcurrentMovesPerNode;

  /** This class keeps track of a scheduled block move */
  public class PendingMove {
    private DBlock block;
    private Source source;
    private DDatanode proxySource;
    private StorageGroup target;

    private PendingMove(Source source, StorageGroup target) {
      this.source = source;
      this.target = target;
    }

    @Override
    public String toString() {
      final Block b = block != null ? block.getBlock() : null;
      String bStr = b != null ? (b + " with size=" + b.getNumBytes() + " ")
          : " ";
      return bStr + "from " + source.getDisplayName() + " to " +
          target.getDisplayName() + " through " + (proxySource != null ? proxySource
          .datanode : "");
    }

    /**
     * @return true if the given block is good for the tentative move.
     */
    private boolean markMovedIfGoodBlock(DBlock block, StorageType targetStorageType) {
      synchronized (block) {
        synchronized (movedBlocks) {
          if (isGoodBlockCandidate(source, target, targetStorageType, block)) {
            this.block = block;
            if (chooseProxySource()) {
              movedBlocks.put(block);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Decided to move " + this);
              }
              return true;
            }
          }
        }
      }
      return false;
    }

    /**
     * Choose a proxy source.
     *
     * @return true if a proxy is found; otherwise false
     */
    private boolean chooseProxySource() {
      return true;
    }

    /** Dispatch the move to the proxy source & wait for the response. */
    private void dispatch() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Start moving " + this);
      }

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
        ExtendedBlock eb = new ExtendedBlock(nnc.getBlockpoolID(),
            block.getBlock());
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
      } catch (IOException e) {
        LOG.warn("Failed to move " + this + ": " + e.getMessage());

        // Proxy or target may have some issues, delay before using these nodes
        // further in order to avoid a potential storm of "threads quota
        // exceeded" warnings when the dispatcher gets out of sync with work
        // going on in datanodes.
        proxySource.activateDelay(delayAfterErrors);

      } finally {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        IOUtils.closeSocket(sock);

        proxySource.removePendingBlock(this);

        synchronized (this) {
          reset();
        }
        synchronized (Dispatcher.this) {
          Dispatcher.this.notifyAll();
        }
      }
    }

    /** Send a block replace request to the output stream */
    private void sendRequest(DataOutputStream out, ExtendedBlock eb,
        Token<BlockTokenIdentifier> accessToken) throws IOException {
      new Sender(out).replaceBlock(eb, target.storageType, accessToken,
          source.getDatanodeInfo().getDatanodeUuid(), proxySource.datanode);
    }

    /** Receive a block copy response from the input stream */
    private void receiveResponse(DataInputStream in) throws IOException {
      BlockOpResponseProto response =
          BlockOpResponseProto.parseFrom(vintPrefixed(in));
      while (response.getStatus() == Status.IN_PROGRESS) {
        // read intermediate responses
        response = BlockOpResponseProto.parseFrom(vintPrefixed(in));
      }
      String logInfo = "block move is failed";
      DataTransferProtoUtil.checkBlockOpStatus(response, logInfo);
    }

    /** reset the object */
    private void reset() {
      block = null;
      source = null;
      proxySource = null;
      target = null;
    }
  }

  /** A class for keeping track of block locations in the dispatcher. */
  public static class DBlock extends MovedBlocks.Locations<StorageGroup> {
    public DBlock(Block block) {
      super(block);
    }
  }

  /** A class that keeps track of a datanode. */
  public static class DDatanode {
    final DatanodeInfo datanode;
    private final EnumMap<StorageType, Source> sourceMap
        = new EnumMap<StorageType, Source>(StorageType.class);
    private final EnumMap<StorageType, StorageGroup> targetMap
        = new EnumMap<StorageType, StorageGroup>(StorageType.class);
    protected long delayUntil = 0L;
    /** blocks being moved but not confirmed yet */
    private final List<PendingMove> pendings;
    private volatile boolean hasFailure = false;
    private final int maxConcurrentMoves;

    @Override
    public String toString() {
      return getClass().getSimpleName() + ":" + datanode;
    }

    public DDatanode(DatanodeInfo datanode, int maxConcurrentMoves) {
      this.datanode = datanode;
      this.maxConcurrentMoves = maxConcurrentMoves;
      this.pendings = new ArrayList<PendingMove>(maxConcurrentMoves);
    }

    public DatanodeInfo getDatanodeInfo() {
      return datanode;
    }

    private static <G extends StorageGroup> void put(StorageType storageType,
        G g, EnumMap<StorageType, G> map) {
      final StorageGroup existing = map.put(storageType, g);
      Preconditions.checkState(existing == null);
    }

    public StorageGroup addTarget(StorageType storageType) {
      final StorageGroup g = new StorageGroup(this.datanode, storageType);
      put(storageType, g, targetMap);
      return g;
    }

    public Source addSource(StorageType storageType, Dispatcher d) {
      final Source s = d.new Source(storageType, this);
      put(storageType, s, sourceMap);
      return s;
    }

    synchronized private void activateDelay(long delta) {
      delayUntil = Time.monotonicNow() + delta;
    }

    synchronized private boolean isDelayActive() {
      if (delayUntil == 0 || Time.monotonicNow() > delayUntil) {
        delayUntil = 0;
        return false;
      }
      return true;
    }

    /** Check if the node can schedule more blocks to move */
    synchronized boolean isPendingQNotFull() {
      return pendings.size() < maxConcurrentMoves;
    }

    /** Check if all the dispatched moves are done */
    synchronized boolean isPendingQEmpty() {
      return pendings.isEmpty();
    }

    /** Add a scheduled block move to the node */
    synchronized boolean addPendingBlock(PendingMove pendingBlock) {
      if (!isDelayActive() && isPendingQNotFull()) {
        return pendings.add(pendingBlock);
      }
      return false;
    }

    /** Remove a scheduled block move from the node */
    synchronized boolean removePendingBlock(PendingMove pendingBlock) {
      return pendings.remove(pendingBlock);
    }

    void setHasFailure() {
      this.hasFailure = true;
    }
  }

  /** A node that can be the sources of a block move */
  public class Source extends StorageGroup {

    /**
     * Source blocks point to the objects in {@link org.apache.hadoop.hdfs.server.balancer.Dispatcher#globalBlocks}
     * because we want to keep one copy of a block and be aware that the
     * locations are changing over time.
     */
    private final List<DBlock> srcBlocks = new ArrayList<DBlock>();

    private Source(StorageType storageType, DDatanode dn) {
      super(dn.datanode, storageType);
    }

    /** Add a pending move */
    public PendingMove addPendingMove(DBlock block, StorageGroup target) {
      return new PendingMove(this, target);
      //return target.addPendingMove(block, new PendingMove(this, target));
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return super.equals(obj);
    }
  }

  public Dispatcher(NameNodeConnector nnc, long movedWinWidth, int moverThreads,
      int dispatcherThreads, int maxConcurrentMovesPerNode, Configuration conf) {
    this.nnc = nnc;
    this.movedWinWidth = movedWinWidth;
    this.movedBlocks = new MovedBlocks<StorageGroup>(movedWinWidth);

    this.cluster = NetworkTopology.getInstance(conf);

    this.moveExecutor = Executors.newFixedThreadPool(moverThreads);
    this.dispatchExecutor = dispatcherThreads == 0? null
        : Executors.newFixedThreadPool(dispatcherThreads);
    this.maxConcurrentMovesPerNode = maxConcurrentMovesPerNode;

    this.saslClient = new SaslDataTransferClient(conf,
        DataTransferSaslUtil.getSaslPropertiesResolver(conf),
        TrustedChannelResolver.getInstance(conf), nnc.fallbackToSimpleAuth);
  }

  public DistributedFileSystem getDistributedFileSystem() {
    return nnc.getDistributedFileSystem();
  }

  public NetworkTopology getCluster() {
    return cluster;
  }

  private boolean shouldIgnore(DatanodeInfo dn) {
    // ignore decommissioned nodes
    final boolean decommissioned = dn.isDecommissioned();
    // ignore decommissioning nodes
    final boolean decommissioning = dn.isDecommissionInProgress();

    if (decommissioned || decommissioning) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Excluding datanode " + dn + ": " + decommissioned + ", "
            + decommissioning);
      }
      return true;
    }
    return false;
  }

  /** Get live datanode storage reports and then build the network topology. */
  public List<DatanodeStorageReport> init() throws IOException {
    final DatanodeStorageReport[] reports = nnc.getLiveDatanodeStorageReport();
    final List<DatanodeStorageReport> trimmed = new ArrayList<DatanodeStorageReport>();
    // create network topology and classify utilization collections:
    // over-utilized, above-average, below-average and under-utilized.
    for (DatanodeStorageReport r : DFSUtil.shuffle(reports)) {
      final DatanodeInfo datanode = r.getDatanodeInfo();
      if (shouldIgnore(datanode)) {
        continue;
      }
      trimmed.add(r);
      cluster.add(datanode);
    }
    return trimmed;
  }

  public DDatanode newDatanode(DatanodeInfo datanode) {
    return new DDatanode(datanode, maxConcurrentMovesPerNode);
  }

  public void executePendingMove(final PendingMove p) {
    // move the block
    moveExecutor.execute(new Runnable() {
      @Override
      public void run() {
        p.dispatch();
      }
    });
  }

  /** The sleeping period before checking if block move is completed again */
  static private long blockMoveWaitTime = 1000L;

  /**
   * Wait for all block move confirmations.
   * @return true if there is failed move execution
   */
  public static boolean waitForMoveCompletion(
      Iterable<? extends StorageGroup> targets) {
    /*boolean hasFailure = false;
    for(;;) {
      boolean empty = true;
      for (StorageGroup t : targets) {
        if (!t.getDDatanode().isPendingQEmpty()) {
          empty = false;
          break;
        } else {
          hasFailure |= t.getDDatanode().hasFailure;
        }
      }
      if (empty) {
        return hasFailure; // all pending queues are empty
      }
      try {
        Thread.sleep(blockMoveWaitTime);
      } catch (InterruptedException ignored) {
      }
    }*/
    return true;
  }

  /**
   * Decide if the block is a good candidate to be moved from source to target.
   * A block is a good candidate if
   * 1. the block is not in the process of being moved/has not been moved;
   * 2. the block does not have a replica on the target;
   * 3. doing the move does not reduce the number of racks that the block has
   */
  private boolean isGoodBlockCandidate(StorageGroup source, StorageGroup target,
                                       StorageType targetStorageType, DBlock block) {
    if (source.equals(target)) {
      return false;
    }
    if (target.storageType != targetStorageType) {
      return false;
    }
    // check if the block is moved or not
    /*if (movedBlocks.contains(block.getBlock())) {
      return false;
    }*/
    final DatanodeInfo targetDatanode = target.getDatanodeInfo();
    if (source.getDatanodeInfo().equals(targetDatanode)) {
      // the block is moved inside same DN
      return true;
    }

    // check if block has replica in target node
    for (StorageGroup blockLocation : block.getLocations()) {
      if (blockLocation.getDatanodeInfo().equals(targetDatanode)) {
        return false;
      }
    }

    if (cluster.isNodeGroupAware()
        && isOnSameNodeGroupWithReplicas(source, target, block)) {
      return false;
    }
    if (reduceNumOfRacks(source, target, block)) {
      return false;
    }
    return true;
  }

  /**
   * Determine whether moving the given block replica from source to target
   * would reduce the number of racks of the block replicas.
   */
  private boolean reduceNumOfRacks(StorageGroup source, StorageGroup target,
                                   DBlock block) {
    final DatanodeInfo sourceDn = source.getDatanodeInfo();
    if (cluster.isOnSameRack(sourceDn, target.getDatanodeInfo())) {
      // source and target are on the same rack
      return false;
    }
    boolean notOnSameRack = true;
    synchronized (block) {
      for (StorageGroup loc : block.getLocations()) {
        if (cluster.isOnSameRack(loc.getDatanodeInfo(), target.getDatanodeInfo())) {
          notOnSameRack = false;
          break;
        }
      }
    }
    if (notOnSameRack) {
      // target is not on the same rack as any replica
      return false;
    }
    for (StorageGroup g : block.getLocations()) {
      if (g != source && cluster.isOnSameRack(g.getDatanodeInfo(), sourceDn)) {
        // source is on the same rack of another replica
        return false;
      }
    }
    return true;
  }

  /**
   * Check if there are any replica (other than source) on the same node group
   * with target. If true, then target is not a good candidate for placing
   * specific replica as we don't want 2 replicas under the same nodegroup.
   *
   * @return true if there are any replica (other than source) on the same node
   *         group with target
   */
  private boolean isOnSameNodeGroupWithReplicas(StorageGroup source,
                                                StorageGroup target, DBlock block) {
    final DatanodeInfo targetDn = target.getDatanodeInfo();
    for (StorageGroup g : block.getLocations()) {
      if (g != source && cluster.isOnSameNodeGroup(g.getDatanodeInfo(), targetDn)) {
        return true;
      }
    }
    return false;
  }

  /** Reset all fields in order to prepare for the next iteration */
  void reset(Configuration conf) {
    cluster = NetworkTopology.getInstance(conf);
    movedBlocks = new MovedBlocks<StorageGroup>(movedWinWidth);
    //movedBlocks.cleanup();
  }

  /** shutdown thread pools */
  public void shutdownNow() {
    if (dispatchExecutor != null) {
      dispatchExecutor.shutdownNow();
    }
    moveExecutor.shutdownNow();
  }
}
