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
package org.smartdata.actions.hdfs;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.actions.ActionStatus;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Test for CheckStorageAction.
 */
public class TestCheckStorageAction extends ActionMiniCluster {
  @Test
  public void testCheckStorageAction() throws IOException {
    CheckStorageAction checkStorageAction = new CheckStorageAction();
    checkStorageAction.setDfsClient(dfsClient);
    checkStorageAction.setContext(smartContext);
    final String file = "/testParallelMovers/file1";
    dfsClient.mkdirs("/testParallelMovers");
    dfsClient.setStoragePolicy("/testParallelMovers", "ONE_SSD");

    // write to HDFS
    final OutputStream out = dfsClient.create(file, true);
    byte[] content = ("This is a file containing two blocks" +
        "......................").getBytes();
    out.write(content);
    out.close();

    // do CheckStorageAction
    checkStorageAction.init(new String[] {file});
    checkStorageAction.run();
    ActionStatus actionStatus = checkStorageAction.getActionStatus();
    System.out.println(StringUtils.formatTime(actionStatus.getRunningTime()));
    Assert.assertTrue(actionStatus.isFinished());
    Assert.assertTrue(actionStatus.isSuccessful());
    Assert.assertEquals(1.0f, actionStatus.getPercentage(), 0.00001f);

    ByteArrayOutputStream resultStream = actionStatus.getResultStream();
    System.out.println(resultStream);
  }
}
