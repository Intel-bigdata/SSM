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

package org.smartdata.alluxio.action;

import static org.junit.Assert.*;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.master.file.FileSystemMaster;
import alluxio.util.io.PathUtils;
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
    Configuration.set(PropertyKey.WEB_RESOURCES,
            PathUtils.concatPath(System.getProperty("user.dir"), "src/test/webapp"));
    mLocalAlluxioCluster.start();
    fs = mLocalAlluxioCluster.getClient();
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
    CreateFileOptions options = CreateFileOptions.defaults().setWriteType(
        WriteType.MUST_CACHE);
    FileOutStream fos = fs.createFile(new AlluxioURI("/dir1/file1"), options);
    fos.write(new byte[] { 1 });
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

  @Test
  public void testLoadAction() throws Exception {
    // write a file but not loaded in cache
    fs.createDirectory(new AlluxioURI("/dir1"));
    CreateFileOptions options = CreateFileOptions.defaults().setWriteType(
        WriteType.THROUGH);
    FileOutStream fos = fs.createFile(new AlluxioURI("/dir1/file1"), options);
    fos.write(new byte[] { 1 });
    fos.close();

    // check file is not cached
    URIStatus status1 = fs.getStatus(new AlluxioURI("/dir1/file1"));
    assertEquals(0, status1.getInMemoryPercentage());

    // run load action
    LoadAction loadAction = new LoadAction();
    Map<String, String> args = new HashMap<>();
    args.put("-path", "/dir1/file1");
    loadAction.init(args);
    loadAction.execute();

    // check file cached status
    URIStatus status2 = fs.getStatus(new AlluxioURI("/dir1/file1"));
    assertEquals(100, status2.getInMemoryPercentage());
  }

  @Test
  public void testFreeAction() throws Exception {
    // write a file and loaded in cache
    fs.createDirectory(new AlluxioURI("/dir1"));
    CreateFileOptions options = CreateFileOptions.defaults().setWriteType(
        WriteType.CACHE_THROUGH);
    FileOutStream fos = fs.createFile(new AlluxioURI("/dir1/file1"), options);
    fos.write(new byte[] { 1 });
    fos.close();

    // check file is cached
    URIStatus status1 = fs.getStatus(new AlluxioURI("/dir1/file1"));
    assertEquals(100, status1.getInMemoryPercentage());

    // run load action
    FreeAction freeAction = new FreeAction();
    Map<String, String> args = new HashMap<>();
    args.put("-path", "/dir1/file1");
    freeAction.init(args);
    freeAction.execute();
    // sleep to wait cache freed
    Thread.sleep(2000);
    // check file cached status
    URIStatus status2 = fs.getStatus(new AlluxioURI("/dir1/file1"));
    assertEquals(0, status2.getInMemoryPercentage());
  }

  @Test
  public void testSetTTLAction() throws Exception {
    // write a file and loaded in cache
    fs.createDirectory(new AlluxioURI("/dir1"));
    CreateFileOptions options = CreateFileOptions.defaults().setWriteType(
        WriteType.CACHE_THROUGH);
    FileOutStream fos = fs.createFile(new AlluxioURI("/dir1/file1"), options);
    fos.write(new byte[] { 1 });
    fos.close();

    // check file is cached
    URIStatus status1 = fs.getStatus(new AlluxioURI("/dir1/file1"));
    assertEquals(-1, status1.getTtl());

    // run ttl action
    SetTTLAction setTTLAction = new SetTTLAction();
    Map<String, String> args = new HashMap<>();
    args.put("-path", "/dir1/file1");
    args.put("TTL", "10000");
    setTTLAction.init(args);
    setTTLAction.execute();

    // check file cached status
    URIStatus status2 = fs.getStatus(new AlluxioURI("/dir1/file1"));
    assertEquals(10000, status2.getTtl());
  }

  @Test
  public void testPinUnpinAction() throws Exception {
    // write a file and loaded in cache
    fs.createDirectory(new AlluxioURI("/dir1"));
    CreateFileOptions options = CreateFileOptions.defaults().setWriteType(
        WriteType.CACHE_THROUGH);
    FileOutStream fos = fs.createFile(new AlluxioURI("/dir1/file1"), options);
    fos.write(new byte[] { 1 });
    fos.close();

    // check file not pinned
    URIStatus status1 = fs.getStatus(new AlluxioURI("/dir1/file1"));
    Set<Long> pinSet1 = mLocalAlluxioCluster.getLocalAlluxioMaster().getMasterProcess()
        .getMaster(FileSystemMaster.class).getPinIdList();
    assertFalse(pinSet1.contains(status1.getFileId()));

    // run pin action
    PinAction pinAction = new PinAction();
    Map<String, String> args = new HashMap<>();
    args.put("-path", "/dir1/file1");
    pinAction.init(args);
    pinAction.execute();

    // check file pinned
    Set<Long> pinSet2 = mLocalAlluxioCluster.getLocalAlluxioMaster().getMasterProcess()
        .getMaster(FileSystemMaster.class).getPinIdList();
    assertTrue(pinSet2.contains(status1.getFileId()));

    // run unpin action
    UnpinAction unpinAction = new UnpinAction();
    Map<String, String> args1 = new HashMap<>();
    args1.put("-path", "/dir1/file1");
    unpinAction.init(args1);
    unpinAction.execute();

    // check file unpinned
    Set<Long> pinSet3 = mLocalAlluxioCluster.getLocalAlluxioMaster().getMasterProcess()
        .getMaster(FileSystemMaster.class).getPinIdList();
    assertFalse(pinSet3.contains(status1.getFileId()));

  }
}
