package org.smartdata.actions.hdfs;


import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.actions.ActionStatus;

import java.util.UUID;

/**
 * Check and return file blocks storage location.
 */
public class CheckStorageAction extends HdfsAction {
  private static final Logger LOG = LoggerFactory.getLogger(WriteFileAction.class);

  private String name = "CheckStorageAction";
  private String fileName;

  public String getName() {
    return name;
  }

  @Override
  public void init(String[] args) {
    super.init(args);
    fileName = args[0];
  }

  @Override
  protected void execute() {
    ActionStatus actionStatus = getActionStatus();
    actionStatus.setStartTime(Time.monotonicNow());
    try {
      HdfsFileStatus fileStatus = dfsClient.getFileInfo(fileName);
      long length = fileStatus.getLen();
      BlockLocation[] blockLocations = dfsClient.getBlockLocations(fileName,
          0, length);
      for (BlockLocation blockLocation : blockLocations) {
        String hosts = new String();
        hosts.concat("{");
        for (String host : blockLocation.getHosts()) {
          hosts.concat(host + " ");
        }
        hosts.concat("}");
        actionStatus.writeResultStream(hosts.getBytes());
      }
    } catch (Exception e) {
      actionStatus.setSuccessful(false);
      throw new RuntimeException(e);
    } finally {
      actionStatus.setFinished(true);
    }
    actionStatus.setSuccessful(true);
  }
}
