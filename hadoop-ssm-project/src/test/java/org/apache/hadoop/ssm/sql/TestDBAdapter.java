package org.apache.hadoop.ssm.sql;

import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.junit.Assert;
import org.junit.Test;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

public class TestDBAdapter {
  @Test
  public void testGetAccessCount() throws Exception {
    Connection conn = new TestDBUtil().getTestDBInstance();
    DBAdapter dbAdapter = new DBAdapter(conn);
    Map<Long, Integer> ret = dbAdapter.getAccessCount(1490932740000l,
        1490936400000l, null);
    Assert.assertTrue(ret.get(2l) == 32);
    if (conn != null) {
      conn.close();
    }
  }

  @Test
  public void testGetFiles () throws Exception {
    Connection conn = new TestDBUtil().getTestDBInstance();
    DBAdapter dbAdapter = new DBAdapter(conn);
    HdfsFileStatus hdfsFileStatus = dbAdapter.getFile(56);
    Assert.assertTrue(hdfsFileStatus.getLen() == 20484l);
    HdfsFileStatus hdfsFileStatus1 = dbAdapter.getFile("/des");
    Assert.assertTrue(hdfsFileStatus1.getAccessTime() == 1490936390000l);
    if (conn != null) {
      conn.close();
    }
  }

  @Test
  public void testGetStorageCapacity () throws Exception {
    Connection conn = new TestDBUtil().getTestDBInstance();
    DBAdapter dbAdapter = new DBAdapter(conn);
    StorageCapacity storageCapacity = dbAdapter.getStorageCapacity("HDD");
    Assert.assertTrue(storageCapacity.getCapacity() == 65536000l);
    if (conn != null) {
      conn.close();
    }
  }

  @Test
  public void testGetCachedFileStatus () throws Exception {
    Connection conn = new TestDBUtil().getTestDBInstance();
    DBAdapter dbAdapter = new DBAdapter(conn);
    CachedFileStatus cachedFileStatus = dbAdapter.getCachedFileStatus(6);
    Assert.assertTrue(cachedFileStatus.getFromTime() == 1490918400000l);
    List<CachedFileStatus> cachedFileList = dbAdapter.getCachedFileStatus();
    Assert.assertTrue(cachedFileList.get(0).getFid() == 6);
    Assert.assertTrue(cachedFileList.get(1).getFid() == 19);
    Assert.assertTrue(cachedFileList.get(2).getFid() == 23);
    if (conn != null) {
      conn.close();
    }
  }

  @Test
  public void testGetErasureCodingPolicy () throws Exception {
    Connection conn = new TestDBUtil().getTestDBInstance();
    DBAdapter dbAdapter = new DBAdapter(conn);
    ErasureCodingPolicy erasureCodingPolicy = dbAdapter.getErasureCodingPolicy(4);
    Assert.assertEquals(erasureCodingPolicy.getCodecName(),"xor");
    if (conn != null) {
      conn.close();
    }
  }
}
