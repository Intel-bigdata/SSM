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

package org.smartdata.alluxio.action.metric.fetcher;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.master.LocalAlluxioCluster;
import alluxio.util.io.PathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.smartdata.alluxio.metric.fetcher.AlluxioNamespaceFetcher;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.TestDaoUtil;
import org.smartdata.model.FileInfo;

import java.io.IOException;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestAlluxioNamespaceFetcher extends TestDaoUtil {
  LocalAlluxioCluster mLocalAlluxioCluster;
  FileSystem fs;
  MetaStore metaStore;

  @Before
  public void setUp() throws Exception {
    mLocalAlluxioCluster = new LocalAlluxioCluster(2);
    mLocalAlluxioCluster.initConfiguration();
    Configuration.set(PropertyKey.WEB_RESOURCES,
            PathUtils.concatPath(System.getProperty("user.dir"), "src/test/webapp"));
    mLocalAlluxioCluster.start();
    fs = mLocalAlluxioCluster.getClient();
    initDao();
    metaStore = new MetaStore(druidPool);
  }

  @After
  public void tearDown() throws Exception {
    if (mLocalAlluxioCluster != null) {
      mLocalAlluxioCluster.stop();
    }
    closeDao();
  }

  @Test
  public void testNamespaceFetcher() throws Exception {
    // create namespace:
    // /dir1
    // /dir2
    //    -dir21
    //    -dir22
    //        -file221
    //        -file222
    //    -file21
    // /dir3
    //    -file31
    
    fs.createDirectory(new AlluxioURI("/dir1"));
    fs.createDirectory(new AlluxioURI("/dir2"));
    fs.createDirectory(new AlluxioURI("/dir3"));
    fs.createDirectory(new AlluxioURI("/dir2/dir21"));
    fs.createDirectory(new AlluxioURI("/dir2/dir22"));

    createFile("/dir3/file31");
    createFile("/dir2/dir22/file221");
    createFile("/dir2/dir22/file222");
    createFile("/dir2/file21");

    AlluxioNamespaceFetcher fetcher = new AlluxioNamespaceFetcher(fs, metaStore, 100,
        Executors.newScheduledThreadPool(4));
    fetcher.startFetch();
    
    //wait complete
    while (!fetcher.fetchFinished()) {
      Thread.sleep(1000);
    }

    Thread.sleep(2000);

    assertEquals(10, metaStore.getFile().size());
    
    FileInfo dir1 = metaStore.getFile("/dir2/dir22");
    assertTrue(dir1 != null);
    assertTrue(dir1.isdir());

    FileInfo file1 = metaStore.getFile("/dir2/dir22/file221");
    assertTrue(file1 != null);
    assertFalse(file1.isdir());
    assertEquals(1, file1.getBlockReplication());
    
  }

  private void createFile(String path) {
    CreateFileOptions options = CreateFileOptions.defaults().setWriteType(
        WriteType.MUST_CACHE);
    FileOutStream fos = null;
    try {
      fos = fs.createFile(new AlluxioURI(path), options);
      fos.write(new byte[] { 1 });
    } catch (IOException | AlluxioException e) {
      e.printStackTrace();
    } finally {
      if (fos != null) {
        try {
          fos.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
