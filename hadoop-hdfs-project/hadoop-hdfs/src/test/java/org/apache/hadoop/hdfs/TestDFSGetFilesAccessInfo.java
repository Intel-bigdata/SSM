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
import org.apache.hadoop.hdfs.protocol.FilesAccessInfo;
import org.apache.hadoop.hdfs.protocol.NNEvent;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;


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
      Map<String, Integer> accessMap = info.getFilesAccessedHashMap();
      assertEquals(numOpen, accessMap.get(filePath).intValue());
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
      Map<String, Integer> accessMap = info.getFilesAccessedHashMap();
      for (int i = 0; i < files.length; i++) {
        Integer acc = accessMap.get(files[i]);
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
    Map<String, Integer> accessMap;
    try {
      info = fs.dfs.getFilesAccessInfo();
      accessMap = info.getFilesAccessedHashMap();
      for (int i = 0; i < files.length; i++) {
        Integer acc = accessMap.get(files[i]);
        assertEquals(numAccess[i], acc == null ? 0 : acc.intValue());
      }

      for (int i = 0; i < files.length; i++) {
        for (int j = 0; j < numAccess[i]; j++) {
          DFSInputStream fin = fs.dfs.open(files[i]);
          fin.close();
        }
      }

      info = fs.dfs.getFilesAccessInfo();
      accessMap = info.getFilesAccessedHashMap();
      for (int i = 0; i < files.length; i++) {
        Integer acc = accessMap.get(files[i]);
        assertEquals(numAccess[i], acc == null ? 0 : acc.intValue());
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=60000)
  public void testMultiFilesRename()
      throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    DistributedFileSystem fs = cluster.getFileSystem();
    String[] files = new String[]{"/B1", "/A2", "/B2", "/A1"};
    String[] desFiles = new String[files.length];
    for(int i = 0; i < files.length; i++) {
      desFiles[i] = files[i] + "-rename";
      DFSTestUtil.createFile(fs, new Path(files[i]), 1024, (short) 3, 0);
    }

    FilesAccessInfo info;
    try {
      for (int i = 0; i < files.length; i++) {
        fs.rename(new Path(files[i]), new Path(desFiles[i]));
      }
      info = fs.dfs.getFilesAccessInfo();
      List<NNEvent> events = info.getNnEvents();

      List<NNEvent> renameEvents = new ArrayList<>();
      for (NNEvent event : events) {
        if (event.getEventType() == NNEvent.EV_RENAME) {
          renameEvents.add(event);
        }
      }

      assertEquals(files.length, renameEvents.size());

      for (int i = 0; i < files.length; i++) {
        NNEvent event = renameEvents.get(i);
        assertEquals(files[i], event.getArgs()[0]);
        assertEquals(desFiles[i], event.getArgs()[1]);
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=60000)
  public void testMultiFilesDelete()
      throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    DistributedFileSystem fs = cluster.getFileSystem();
    String[] files = new String[]{"/B1", "/A2", "/B2", "/A1"};
    for(String file : files) {
      DFSTestUtil.createFile(fs, new Path(file), 1024, (short) 3, 0);
    }

    FilesAccessInfo info;
    try {
      for (String file : files) {
        fs.delete(new Path(file));
      }
      info = fs.dfs.getFilesAccessInfo();
      List<NNEvent> events = info.getNnEvents();

      List<NNEvent> deleteEvents = new ArrayList<>();
      for (NNEvent event : events) {
        if (event.getEventType() == NNEvent.EV_DELETE) {
          deleteEvents.add(event);
        }
      }

      assertEquals(files.length, deleteEvents.size());

      for (int i = 0; i < files.length; i++) {
        NNEvent event = deleteEvents.get(i);
        assertEquals(files[i], event.getArgs()[0]);
      }
    } finally {
      cluster.shutdown();
    }
  }
}
