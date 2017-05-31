package org.smartdata.actions.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.*;

/**
 * Created by intel on 5/31/17.
 */
public class TestReadFileAction extends TestWriteFileAction {

  @Test
  public void testInit() throws IOException {
    ArrayList<String> args = new ArrayList<>();
    args.add("Test");
    ReadFileAction readFileAction = new ReadFileAction();
    readFileAction.init(args.toArray(new String[args.size()]));
    args.add("1024");
    readFileAction.init(args.toArray(new String[args.size()]));
  }

  @Test
  public void testExecute() throws IOException {
    String filePath = "/testWriteFile/file";
    int size = 66560;
    writeFile(filePath, size);
    String[] args = {filePath};
    ReadFileAction readFileAction = new ReadFileAction();
    readFileAction.setDfsClient(client);
    readFileAction.init(args);
    readFileAction.execute();
  }

}