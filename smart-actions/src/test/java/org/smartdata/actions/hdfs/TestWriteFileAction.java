package org.smartdata.actions.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.actions.ActionStatus;

import java.io.IOException;
import java.util.ArrayList;


public class TestWriteFileAction extends ActionMiniCluster {
  protected void writeFile(String filePath, int length) throws IOException {
    String[] args = {filePath, String.valueOf(length)};
    WriteFileAction writeFileAction = new WriteFileAction();
    writeFileAction.setDfsClient(dfsClient);
    writeFileAction.setContext(smartContext);
    writeFileAction.init(args);
    writeFileAction.run();

    // check results
    ActionStatus actionStatus = writeFileAction.getActionStatus();
    Assert.assertTrue(actionStatus.isFinished());
    Assert.assertTrue(actionStatus.isSuccessful());
    System.out.println("Write file action running time : " +
        StringUtils.formatTime(actionStatus.getRunningTime()));
    Assert.assertEquals(1.0f, actionStatus.getPercentage(), 0.00001f);
  }

  @Test
  public void testInit() throws IOException {
    ArrayList<String> args = new ArrayList<>();
    args.add("Test");
    args.add("10");
    WriteFileAction writeFileAction = new WriteFileAction();
    writeFileAction.init(args.toArray(new String[args.size()]));
    args.add("1024");
    writeFileAction.init(args.toArray(new String[args.size()]));
  }

  @Test
  public void testExecute() throws Exception {
    String filePath = "/testWriteFile/file";
    int size = 66560;
    writeFile(filePath, size);
    HdfsFileStatus fileStatus = dfs.getClient().getFileInfo(filePath);
    Assert.assertTrue(fileStatus.getLen() == size);
  }
}
