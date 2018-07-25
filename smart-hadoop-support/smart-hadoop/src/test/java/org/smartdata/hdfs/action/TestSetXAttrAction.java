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

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.action.MockActionStatusReporter;
import org.smartdata.hdfs.MiniClusterHarness;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestSetXAttrAction extends MiniClusterHarness {
  private String testAttName = "user.coldloc";
  protected void setXAttr(String srcPath, String attName, String attValue) {
    Map<String, String> args = new HashMap<>();
    args.put(SetXAttrAction.FILE_PATH, srcPath);
    args.put(SetXAttrAction.ATT_NAME, "" + attName);
    args.put(SetXAttrAction.ATT_VALUE, "" + attValue);
    SetXAttrAction setXAttrAction = new SetXAttrAction();
    setXAttrAction.setDfsClient(dfsClient);
    setXAttrAction.setContext(smartContext);
    setXAttrAction.init(args);
    setXAttrAction.setStatusReporter(new MockActionStatusReporter());
    setXAttrAction.run();
  }

  @Test
  public void testInit() throws IOException {
    SetXAttrAction setXAttrAction = new SetXAttrAction();
    setXAttrAction.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args = new HashMap<>();
    args.put(SetXAttrAction.FILE_PATH, "Test");
    setXAttrAction.init(args);
    args.put(SetXAttrAction.ATT_NAME, testAttName);
    args.put(SetXAttrAction.ATT_VALUE, "test");
    setXAttrAction.init(args);
  }

  @Test
  public void testSetAttr() throws IOException {
    String srcPath = "/setxattrFile";
    String randomValue = RandomStringUtils.randomAlphanumeric(17)
        .toUpperCase();
    Path src = new Path(srcPath);
    DFSTestUtil.createFile(dfs, src, 100, (short)3, 0xFEED);
    setXAttr(srcPath, testAttName, randomValue);
    String result = new String(dfsClient.getXAttr(srcPath, testAttName));
    Assert.assertEquals(result, randomValue);
  }
}
