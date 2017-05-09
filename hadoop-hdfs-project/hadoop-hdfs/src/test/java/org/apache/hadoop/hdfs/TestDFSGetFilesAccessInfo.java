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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FileAccessEvent;
import org.apache.hadoop.hdfs.protocol.FilesAccessInfo;
import org.apache.hadoop.hdfs.protocol.NNEvent;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestDFSGetFilesAccessInfo {
  @Test(timeout=60000)
  public void testMultiAccess() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    DistributedFileSystem fs = cluster.getFileSystem();
    String filePath = "/testfile";
    DFSTestUtil.createFile(fs, new Path(filePath), 1024, (short) 3, 0);

    int numOpen = 4;
    for (int i = 0; i < numOpen; i++) {
      DFSInputStream fin = fs.dfs.open(filePath);
      fin.close();
    }
    try {
      FilesAccessInfo info = fs.dfs.getFilesAccessInfo();
      List<FileAccessEvent> events = info.getFileAccessEvents();
      assertEquals(numOpen, events.size());
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=60000)
  public void testMultiAccessMultiFiles() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    DistributedFileSystem fs = cluster.getFileSystem();
    String[] files = new String[]{"/B1", "/B2", "/A1", "/A2"};
    for(String file : files) {
      DFSTestUtil.createFile(fs, new Path(file), 1024, (short) 3, 0);
    }

    int[] numAccess = new int[files.length];
    for (int i = 0; i < numAccess.length; i++) {
      numAccess[i] = ThreadLocalRandom.current().nextInt(0, 8 + 1);
    }

    for (int i = 0; i < files.length; i++) {
      for (int j = 0; j < numAccess[i]; j++) {
        DFSInputStream fin = fs.dfs.open(files[i]);
        fin.close();
      }
    }

    try {
      FilesAccessInfo info = fs.dfs.getFilesAccessInfo();
      List<FileAccessEvent> events = info.getFileAccessEvents();
      for (int i = 0; i < files.length; i++) {
        String file = files[i];
        Long acc = events.stream().filter(e -> e.getPath().equals(file)).count();
        assertEquals(numAccess[i], acc == null ? 0 : acc.intValue());
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=60000)
  public void testMultiAccessMultiFilesMultiRounds() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    DistributedFileSystem fs = cluster.getFileSystem();
    String[] files = new String[]{"/B1", "/B2", "/A1", "/A2"};
    for(String file : files) {
      DFSTestUtil.createFile(fs, new Path(file), 1024, (short) 3, 0);
    }

    int[] numAccess = new int[files.length];
    for (int i = 0; i < numAccess.length; i++) {
      numAccess[i] = ThreadLocalRandom.current().nextInt(0, 8 + 1);
    }

    for (int i = 0; i < files.length; i++) {
      for (int j = 0; j < numAccess[i]; j++) {
        DFSInputStream fin = fs.dfs.open(files[i]);
        fin.close();
      }
    }

    FilesAccessInfo info;
    List<FileAccessEvent> events;
    try {
      info = fs.dfs.getFilesAccessInfo();
      events = info.getFileAccessEvents();
      long startTime = events.get(0).getTimestamp();
      for (int i = 0; i < files.length; i++) {
        String file = files[i];
        Long acc = events.stream().filter(e -> e.getPath().equals(file)).count();
        assertEquals(numAccess[i], acc == null ? 0 : acc.intValue());
      }

      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        // ignore it
      }

      for (int i = 0; i < files.length; i++) {
        for (int j = 0; j < numAccess[i]; j++) {
          DFSInputStream fin = fs.dfs.open(files[i]);
          fin.close();
        }
      }

      info = fs.dfs.getFilesAccessInfo();
      events = info.getFileAccessEvents();
      long endTime = events.get(events.size() - 1).getTimestamp();
      assertTrue(endTime - startTime > 5000);
      for (int i = 0; i < files.length; i++) {
        String file = files[i];
        Long acc = events.stream().filter(e -> e.getPath().equals(file)).count();
        assertEquals(numAccess[i], acc == null ? 0 : acc.intValue());
      }
    } finally {
      cluster.shutdown();
    }
  }
}
