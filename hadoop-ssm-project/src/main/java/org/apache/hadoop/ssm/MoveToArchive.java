
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
public  class MoveToArchive extends ActionBase {

  private static MoveToArchive instance;
  public static final byte COLD_STORAGE_POLICY_ID = 2;
//  public static final String COLD_STORAGE_POLICY_NAME = "COLD";
  String fileName;
  private DFSClient dfsClient;
  Configuration conf;

  public MoveToArchive(DFSClient client, Configuration conf) {
    super(client);
    this.dfsClient = client;
    this.conf = conf;
  }

  public static synchronized MoveToArchive getInstance(DFSClient dfsClient, Configuration conf) {
    if (instance == null) {
      instance = new MoveToArchive(dfsClient, conf);
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
    if(runArchive(fileName)){
      return true;
    }else{
      return false;
    }
  }

  private boolean runArchive(String fileName) {
    if (getStoragePolicy(fileName) == COLD_STORAGE_POLICY_ID) {
      return true;
    }
    System.out.println("*" + System.currentTimeMillis() + " : " + fileName + " -> " + "archive");
    try {
      dfsClient.setStoragePolicy(fileName, "COLD");
    } catch (Exception e) {
      return false;
    }
    try {
      ToolRunner.run(conf, new Mover.Cli(),
              new String[]{"-p", fileName});
    } catch (Exception e) {
      return false;
    }
    return  true;
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