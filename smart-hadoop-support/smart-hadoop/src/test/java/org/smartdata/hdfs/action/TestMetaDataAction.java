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
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.action.MockActionStatusReporter;
import org.smartdata.hdfs.MiniClusterHarness;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Test for MetaDataAction
 */
public class TestMetaDataAction extends MiniClusterHarness {

  @Test
  public void testLocalMetadataChange() throws IOException {
    final String srcPath = "/test";
    final String file = "file";

    dfs.mkdirs(new Path(srcPath));
    FSDataOutputStream out = dfs.create(new Path(srcPath + "/" + file));
    out.close();

    MetaDataAction metaFileAction = new MetaDataAction();
    metaFileAction.setDfsClient(dfsClient);
    metaFileAction.setContext(smartContext);
    metaFileAction.setStatusReporter(new MockActionStatusReporter());

    Map<String, String> args = new HashMap<>();
    args.put(MetaDataAction.FILE_PATH, srcPath + "/" + file);
    args.put(MetaDataAction.OWNER_NAME, "test");
    args.put(MetaDataAction.PERMISSION, "777");

    metaFileAction.init(args);
    metaFileAction.run();

    Assert.assertTrue(dfs.getFileStatus(new Path(srcPath + "/" + file)).getOwner().equals("test"));
    Assert.assertTrue(dfs.getFileStatus(new Path(srcPath + "/" + file)).getPermission().toString().equals("rwxrwxrwx"));
  }

  @Test
  public void testRemoteMetadataChange() throws IOException {
    final String srcPath = "/test";
    final String file = "file";

    dfs.mkdirs(new Path(srcPath));
    FSDataOutputStream out = dfs.create(new Path(srcPath + "/" + file));
    out.close();

    MetaDataAction metaFileAction = new MetaDataAction();
    metaFileAction.setDfsClient(dfsClient);
    metaFileAction.setContext(smartContext);
    metaFileAction.setStatusReporter(new MockActionStatusReporter());

    Map<String, String> args = new HashMap<>();
    args.put(MetaDataAction.FILE_PATH, dfs.getUri() + srcPath + "/" + file);
    args.put(MetaDataAction.OWNER_NAME, "test");
    args.put(MetaDataAction.PERMISSION, "777");

    metaFileAction.init(args);
    metaFileAction.run();

    Assert.assertTrue(dfs.getFileStatus(new Path(srcPath + "/" + file)).getOwner().equals("test"));
    Assert.assertTrue(dfs.getFileStatus(new Path(srcPath + "/" + file)).getPermission().toString().equals("rwxrwxrwx"));

  }
}
