
package org.apache.hadoop.ssm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.FilesInfo;
import org.apache.hadoop.hdfs.server.mover.Mover;
import org.apache.hadoop.util.ToolRunner;

import static org.apache.hadoop.hdfs.protocol.FilesInfo.STORAGEPOLICY;

/**
 * Created by cc on 17-1-15.
 */
public  class MoveToSSD extends ActionBase {

  private static MoveToSSD instance;
  public static final byte ALLSSD_STORAGE_POLICY_ID = 12;
  private String fileName;
  private DFSClient dfsClient;
  private Configuration conf;

  public MoveToSSD(DFSClient client, Configuration conf) {
    super(client);
    this.dfsClient = client;
    this.conf = conf;
  }

  public static synchronized MoveToSSD getInstance(DFSClient dfsClient, Configuration conf) {
    if (instance == null) {
      instance = new MoveToSSD(dfsClient, conf);
    }
    return instance;
  }

  public void initial(String[] args) {
    this.fileName = args[0];
  }

  /**
   * Execute an action.
   *
   * @return true if success, otherwise return false.
   */
  public boolean execute() {
    if(runSSD(fileName)){
      return true;
    }else{
      return false;
    }
  }

  private boolean runSSD(String fileName) {
    if (getStoragePolicy(fileName) == ALLSSD_STORAGE_POLICY_ID) {
      return true;
    }
    System.out.println("*" + System.currentTimeMillis() + " : " + fileName + " -> " + "ssd");
    try {
      dfsClient.setStoragePolicy(fileName, "ALL_SSD");
    } catch (Exception e) {
      return false;
    }
    try {
      ToolRunner.run(conf, new Mover.Cli(),
              new String[]{"-p", fileName});
    } catch (Exception e) {
      return false;
    }
    return true;
  }


  private byte getStoragePolicy(String fileName) {
    FilesInfo filesInfo;
    String[] paths = {fileName};

    try {
      filesInfo = dfsClient.getFilesInfo(paths, STORAGEPOLICY, false, false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    byte storagePolicy = filesInfo.getStoragePolicy().get(0);
    return storagePolicy;
  }

}