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
package org.smartdata.hdfs.action;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.action.MockActionStatusReporter;
import org.smartdata.hdfs.MiniClusterHarness;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class TestTruncate0Action extends MiniClusterHarness {
  @Test
  public void testSetFileLen() throws IOException, InterruptedException {
    final String srcPath = "/test";
    final String file = "file";

    dfs.mkdirs(new Path(srcPath));
    FSDataOutputStream out = dfs.create(new Path(srcPath + "/" + file));

    for (int i = 0; i < 50; i++) {
      out.writeByte(1);
    }

    out.close();

    dfs.setXAttr(new Path(srcPath + "/" + file), "user.coldloc", "test".getBytes(),
            EnumSet.of(XAttrSetFlag.CREATE, XAttrSetFlag.REPLACE));
    FileStatus oldFileStatus = dfs.getFileStatus(new Path(srcPath + "/" + file));
    Map<String, byte[]> oldXAttrs = dfs.getXAttrs(new Path(srcPath + "/" + file));

    Truncate0Action setLen2ZeroAction = new Truncate0Action();
    setLen2ZeroAction.setDfsClient(dfsClient);
    setLen2ZeroAction.setContext(smartContext);
    setLen2ZeroAction.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args = new HashMap<>();
    args.put(Truncate0Action.FILE_PATH, srcPath + "/" + file);

    setLen2ZeroAction.init(args);
    setLen2ZeroAction.run();

    FileStatus newFileStatus = dfs.getFileStatus(new Path(srcPath + "/" + file));
    Map<String, byte[]> newXAttrs = dfs.getXAttrs(new Path(srcPath + "/" + file));

    Assert.assertTrue(newFileStatus.getLen()==0);
    Assert.assertTrue(oldFileStatus.getOwner().equals(newFileStatus.getOwner()));
    Assert.assertTrue(oldFileStatus.getGroup().equals(newFileStatus.getGroup()));
    Assert.assertTrue(oldFileStatus.getPermission().equals(newFileStatus.getPermission()));
    Assert.assertTrue(oldFileStatus.getReplication()==newFileStatus.getReplication());
    //Assert.assertTrue(oldFileStatus.getAccessTime()==newFileStatus.getAccessTime());
    //Assert.assertTrue(oldFileStatus.getModificationTime()==newFileStatus.getModificationTime());
    Assert.assertTrue(oldXAttrs.size()==newXAttrs.size());
    for(Map.Entry<String, byte[]> oldXAttr : oldXAttrs.entrySet()){
      Assert.assertTrue(Arrays.equals(oldXAttr.getValue(),newXAttrs.get(oldXAttr.getKey())));
    }
  }
}
