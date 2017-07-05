package org.smartdata.model;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by zhendu on 2017/7/5.
 */
public class TestFileInfo {
  private class ChildFileInfo extends FileInfo {

    public ChildFileInfo(String path, long fileId, long length, boolean isdir, short block_replication, long blocksize, long modification_time, long access_time, short permission, String owner, String group, byte storagePolicy) {
      super(path, fileId, length, isdir, block_replication, blocksize, modification_time, access_time, permission, owner, group, storagePolicy);
    }
  }

  @Test
  public void testEquals() throws Exception {
    //Case 1:
    FileInfo fileInfo = new FileInfo(" ", 1, 1, true, (short) 1, 1
            , 1, 1, (short) 1, " ", " ", (byte) 1);
    Assert.assertEquals(true, fileInfo.equals(fileInfo));

    //Case 2:
    FileInfo fileInfo1 = new FileInfo(" ", 1, 1, true, (short) 1, 1
            , 1, 1, (short) 1, " ", " ", (byte) 1);
    Assert.assertEquals(true, fileInfo.equals(fileInfo1));

    //Case 3:
    FileInfo fileInfo2 = new FileInfo(" ", 1, 1, true, (short) 1, 1
            , 1, 1, (short) 1, null, " ", (byte) 1);
    Assert.assertEquals(false, fileInfo.equals(fileInfo2));
    Assert.assertEquals(false, fileInfo2.equals(fileInfo));

    //Case 4:
    FileInfo fileInfo3 = new FileInfo(null, 1, 1, true, (short) 1, 1
            , 1, 1, (short) 1, " ", " ", (byte) 1);
    Assert.assertEquals(false, fileInfo.equals(fileInfo3));
    Assert.assertEquals(false, fileInfo3.equals(fileInfo));


    //Case 5:
    FileInfo fileInfo4 = new FileInfo(null, 1, 1, true, (short) 1, 1
            , 1, 1, (short) 1, " ", null, (byte) 1);
    Assert.assertEquals(false, fileInfo.equals(fileInfo4));
    Assert.assertEquals(false, fileInfo4.equals(fileInfo));

    //Case 6:
    FileInfo fileInfo5 = new FileInfo(" ", 1, 1, true, (short) 1, 1
            , 1, 1, (short) 2, " ", " ", (byte) 1);
    Assert.assertEquals(false, fileInfo.equals(fileInfo5));

    //Case 7;
    FileInfo fileInfo6 = new ChildFileInfo(" ", 1, 1, true, (short) 1, 1
            , 1, 1, (short) 2, " ", " ", (byte) 1);
    Assert.assertEquals(false, fileInfo.equals(fileInfo6));

  }
}
