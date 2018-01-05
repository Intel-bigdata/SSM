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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.SmartContext;
import org.smartdata.action.MockActionStatusReporter;
import org.smartdata.conf.SmartConf;
import org.smartdata.hdfs.MiniClusterHarness;

import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Test for Copy2S3FileAction.
 */
public class TestCopy2S3Action extends MiniClusterHarness {
  //change this to connect with aws s3
  // final String S3_ACCESS_KEY = "";
  // final String S3_SECRET_KEY = "";
  //
  // private SmartContext testContext;
  //
  // @Before
  // public void testinit(){
  //   SmartConf configuration = smartContext.getConf();
  //   testContext = smartContext;
  //   configuration.set("fs.s3a.access.key", S3_ACCESS_KEY);
  //   configuration.set("fs.s3a.secret.key", S3_SECRET_KEY);
  //   testContext.setConf(configuration);
  // }
  //
  // private void copy2S3File(String src, String dest) throws Exception {
  //   Copy2S3Action copy2S3Action = new Copy2S3Action();
  //   copy2S3Action.setDfsClient(dfsClient);
  //   copy2S3Action.setContext(testContext);
  //   copy2S3Action.setStatusReporter(new MockActionStatusReporter());
  //   Map<String, String> args = new HashMap<>();
  //   args.put(Copy2S3Action.FILE_PATH, src);
  //
  //   args.put(Copy2S3Action.DEST, dest);
  //   copy2S3Action.init(args);
  //   copy2S3Action.run();
  // }
  //
  // @Test
  // public void testS3FileCopy() throws Exception {
  //   final String srcPath = "/testCopy";
  //   final String file = "testFile";
  //
  //   String destFile = "s3a://xxxctest/";
  //
  //   //get unigue name with the help of system time
  //   destFile = destFile + System.currentTimeMillis();
  //   dfs.mkdirs(new Path(srcPath));
  //   // write to DISK
  //   final OutputStream out1 = dfsClient.create(srcPath + "/" + file, true);
  //   out1.write(1);
  //   out1.close();
  //   copy2S3File(srcPath + "/" + file, destFile);
  //
  //   //check the file in S3
  //
  //   FileSystem fs = FileSystem.get(URI.create(destFile), testContext.getConf());
  //
  //   Assert.assertTrue(fs.exists(new Path(destFile)));
  // }


}
