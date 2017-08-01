package org.smartdata.actions.hdfs.move;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.net.NetworkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.model.actions.hdfs.Source;
import org.smartdata.model.actions.hdfs.StorageGroup;
import org.smartdata.model.actions.hdfs.StorageMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetMoverSchedulerInfo {
  private DFSClient client;
  private StorageMap storages = new StorageMap();
  private int maxConcurrentMovesPerNode = 5;
  public static final Logger LOG =
      LoggerFactory.getLogger(GetMoverSchedulerInfo.class);

  private NetworkTopology cluster;

  public GetMoverSchedulerInfo(DFSClient client) throws IOException {
    this.client = client;
  }

  public StorageMap getStorages() {
    return storages;
  }

  public void run() {
    try {
      final List<DatanodeStorageReport> reports = init();
      for(DatanodeStorageReport r : reports) {
        // TODO: store data abstracted from reports to MetaStore
        final DDatanode dn = new DDatanode(r.getDatanodeInfo(), maxConcurrentMovesPerNode);
        for(StorageType t : StorageType.getMovableTypes()) {
          final Source source = dn.addSource(t);
          final long maxRemaining = getMaxRemaining(r, t);
          final StorageGroup target = maxRemaining > 0L ? dn.addTarget(t) : null;
          storages.add(source, target);
        }
      }
    } catch (IOException e) {
      LOG.error("Process datanode report error", e);
    }
  }

  private static long getMaxRemaining(DatanodeStorageReport report, StorageType t) {
    long max = 0L;
    for(StorageReport r : report.getStorageReports()) {
      if (r.getStorage().getStorageType() == t) {
        if (r.getRemaining() > max) {
          max = r.getRemaining();
        }
      }
    }
    return max;
  }

  /**
   * Get live datanode storage reports and then build the network topology.
   * @return
   * @throws IOException
   */
  public List<DatanodeStorageReport> init() throws IOException {
    final DatanodeStorageReport[] reports =
        client.getDatanodeStorageReport(DatanodeReportType.LIVE);
    final List<DatanodeStorageReport> trimmed = new ArrayList<DatanodeStorageReport>();
    // create network topology and classify utilization collections:
    // over-utilized, above-average, below-average and under-utilized.
    for (DatanodeStorageReport r : DFSUtil.shuffle(reports)) {
      final DatanodeInfo datanode = r.getDatanodeInfo();
      trimmed.add(r);
    }
    return trimmed;
  }
}
