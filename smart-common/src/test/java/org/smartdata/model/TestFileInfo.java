/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.smartdata.model;

import org.junit.Assert;
import org.junit.Test;


public class TestFileInfo {
  private class ChildFileInfo extends FileInfo {
    public ChildFileInfo(
        String path,
        long fileId,
        long length,
        boolean isdir,
        short blockReplication,
        long blocksize,
        long modificationTime,
        long accessTime,
        short permission,
        String owner,
        String group,
        byte storagePolicy) {
      super(
          path,
          fileId,
          length,
          isdir,
          blockReplication,
          blocksize,
          modificationTime,
          accessTime,
          permission,
          owner,
          group,
          storagePolicy);
    }
  }

  @Test
  public void testEquals() throws Exception {
    //Case 1:
    FileInfo fileInfo =
        new FileInfo(" ", 1, 1, true, (short) 1, 1, 1, 1, (short) 1, " ", " ", (byte) 1);
    Assert.assertEquals(true, fileInfo.equals(fileInfo));

    //Case 2:
    FileInfo fileInfo1 =
        new FileInfo(" ", 1, 1, true, (short) 1, 1, 1, 1, (short) 1, " ", " ", (byte) 1);
    Assert.assertEquals(true, fileInfo.equals(fileInfo1));

    //Case 3:
    FileInfo fileInfo2 =
        new FileInfo(" ", 1, 1, true, (short) 1, 1, 1, 1, (short) 1, null, " ", (byte) 1);

    Assert.assertEquals(false, fileInfo.equals(fileInfo2));
    Assert.assertEquals(false, fileInfo2.equals(fileInfo));

    //Case 4:
    FileInfo fileInfo3 =
        new FileInfo(null, 1, 1, true, (short) 1, 1, 1, 1, (short) 1, " ", " ", (byte) 1);

    Assert.assertEquals(false, fileInfo.equals(fileInfo3));
    Assert.assertEquals(false, fileInfo3.equals(fileInfo));

    //Case 5:
    FileInfo fileInfo4 =
        new FileInfo(null, 1, 1, true, (short) 1, 1, 1, 1, (short) 1, " ", null, (byte) 1);
    Assert.assertEquals(false, fileInfo.equals(fileInfo4));
    Assert.assertEquals(false, fileInfo4.equals(fileInfo));

    //Case 6:
    FileInfo fileInfo5 =
        new FileInfo(" ", 1, 1, true, (short) 1, 1, 1, 1, (short) 2, " ", " ", (byte) 1);
    Assert.assertEquals(false, fileInfo.equals(fileInfo5));
  }
}
