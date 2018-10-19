package org.smartdata.hdfs.action;

import org.junit.Assert;
import org.junit.Test;
import org.smartdata.hdfs.MiniClusterHarness;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class TestCheckSumAction extends MiniClusterHarness {
  @Test
  public void testCheckSumAction() throws IOException{
    CheckSumAction checkSumAction = new CheckSumAction();
    checkSumAction.setDfsClient(dfsClient);
    checkSumAction.setContext(smartContext);
    final String file = "/testPath/file1";
    dfsClient.mkdirs("/testPath");
    dfsClient.setStoragePolicy("/testPath", "ONE_SSD");

    // write to HDFS
    final OutputStream out = dfsClient.create(file, true);
    byte[] content = ("This is a file containing two blocks" +
        "......................").getBytes();
    out.write(content);
    out.close();

    Map<String, String> args = new HashMap();
    args.put(CheckSumAction.FILE_PATH, file);
    checkSumAction.init(args);
    checkSumAction.run();
    System.out.print(checkSumAction.getActionStatus().getResult());
  }
}
