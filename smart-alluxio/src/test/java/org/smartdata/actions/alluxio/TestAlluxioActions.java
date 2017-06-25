package org.smartdata.actions.alluxio;

import static org.junit.Assert.*;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.master.LocalAlluxioCluster;


public class TestAlluxioActions {
  LocalAlluxioCluster mLocalAlluxioCluster;
  FileSystem fs;
  @Before
  public void setUp() throws Exception {
    System.out.println("setUp .b");
    mLocalAlluxioCluster = new LocalAlluxioCluster(2);
    System.out.println("setUp .a");
    mLocalAlluxioCluster.initConfiguration();
    System.out.println("setUp ");
    mLocalAlluxioCluster.start();
    fs = FileSystem.Factory.get();
    System.out.println("setUp ......");
  }

  @After
  public void tearDown() throws Exception {
    System.out.println("tearDown");
    if (mLocalAlluxioCluster != null) {
      mLocalAlluxioCluster.stop();
    }
    System.out.println("tearDown oooooo");
  }

  @Test
  public void testPersistAction() throws Exception {
    // write a file and not persisted
    fs.createDirectory(new AlluxioURI("/dir1"));
    CreateFileOptions options = CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE);
    FileOutStream fos = fs.createFile(new AlluxioURI("/dir1/file1"), options);
    fos.write(new byte[] {1});
    fos.close();
    System.out.println("AAAAA");

    // check status not persisted
    URIStatus status1 = fs.getStatus(new AlluxioURI("/dir1/file1"));
    assertEquals(status1.getPersistenceState(), "NOT_PERSISTED");
    System.out.println("BBBBB");

    // run persist action
    PersistAction persistAction = new PersistAction();
    Map<String, String> args = new HashMap<>();
    args.put("-path", "/dir1/file1");
    persistAction.init(args);
    persistAction.execute();

    System.out.println("CCCCCC");
    
    // check status persisted
    URIStatus status2 = fs.getStatus(new AlluxioURI("/dir1/file1"));
    assertEquals(status2.getPersistenceState(), "PERSISTED");
  }
}
