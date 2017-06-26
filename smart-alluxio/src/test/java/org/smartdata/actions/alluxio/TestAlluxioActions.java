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
    mLocalAlluxioCluster = new LocalAlluxioCluster(2);
    mLocalAlluxioCluster.initConfiguration();
    mLocalAlluxioCluster.start();
    fs = FileSystem.Factory.get();
  }

  @After
  public void tearDown() throws Exception {
    if (mLocalAlluxioCluster != null) {
      mLocalAlluxioCluster.stop();
    }
  }
  
  @Test
  public void testPersistAction() throws Exception {
    // write a file and not persisted
    fs.createDirectory(new AlluxioURI("/dir1"));
    CreateFileOptions options = CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE);
    FileOutStream fos = fs.createFile(new AlluxioURI("/dir1/file1"), options);
    fos.write(new byte[] {1});
    fos.close();

    // check status not persisted
    URIStatus status1 = fs.getStatus(new AlluxioURI("/dir1/file1"));
    assertEquals(status1.getPersistenceState(), "NOT_PERSISTED");
    
    // run persist action
    PersistAction persistAction = new PersistAction();
    Map<String, String> args = new HashMap<>();
    args.put("-path", "/dir1/file1");
    persistAction.init(args);
    persistAction.execute();
    
    // check status persisted
    URIStatus status2 = fs.getStatus(new AlluxioURI("/dir1/file1"));
    assertEquals(status2.getPersistenceState(), "PERSISTED");
  }
}
