package org.smartdata.actions.hdfs;

import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.actions.ActionStatus;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class TestReadFileAction extends ActionMiniCluster {
  protected void writeFile(String filePath, int length) throws IOException {
    try {
      int bufferSize = 64 * 1024;
      final OutputStream out = dfsClient.create(filePath, true);
      // generate random data with given length
      byte[] buffer = new byte[bufferSize];
      new Random().nextBytes(buffer);
      // write to HDFS
      for (int pos = 0; pos < length; pos += bufferSize) {
        int writeLength = pos + bufferSize < length ? bufferSize : length - pos;
        out.write(buffer, 0, writeLength);
      }
      out.close();
    } catch (IOException e) {
      System.err.println("WriteFile Action fails!\n" + e.getMessage());
    }
  }

  @Test
  public void testInit() throws IOException {
    ReadFileAction readFileAction = new ReadFileAction();
    Map<String, String> args = new HashMap();
    args.put(ReadFileAction.FILE_PATH, "Test");
    readFileAction.init(args);
    args.put(ReadFileAction.BUF_SIZE, "4096");
    readFileAction.init(args);
  }

  @Test
  public void testExecute() throws IOException {
    String filePath = "/testWriteFile/file";
    int size = 66560;
    writeFile(filePath, size);
    ReadFileAction readFileAction = new ReadFileAction();
    readFileAction.setDfsClient(dfsClient);
    readFileAction.setContext(smartContext);
    Map<String, String> args = new HashMap();
    args.put(ReadFileAction.FILE_PATH, filePath);
    readFileAction.init(args);
    readFileAction.run();

    // check results
    ActionStatus actionStatus = readFileAction.getActionStatus();
    Assert.assertTrue(actionStatus.isFinished());
    Assert.assertTrue(actionStatus.isSuccessful());
    System.out.println("Read file action running time : " +
        StringUtils.formatTime(actionStatus.getRunningTime()));
    Assert.assertEquals(1.0f, actionStatus.getPercentage(), 0.00001f);
  }
}
