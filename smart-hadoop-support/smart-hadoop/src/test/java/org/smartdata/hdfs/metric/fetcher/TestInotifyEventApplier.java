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
package org.smartdata.hdfs.metric.fetcher;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.metastore.MetaStore;

import org.smartdata.metastore.TestDaoUtil;
import org.smartdata.model.BackUpInfo;
import org.smartdata.model.FileDiff;
import org.smartdata.model.FileDiffType;
import org.smartdata.model.FileInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestInotifyEventApplier extends TestDaoUtil {
  private MetaStore metaStore = null;
  @Before
  public void init() throws Exception {
    initDao();
    metaStore = new MetaStore(druidPool);
    metaStore.addGroup("cg1");
    metaStore.addUser("user1");
    metaStore.addGroup("cg2");
    metaStore.addUser("user2");
  }

  @Test
  public void testApplier() throws Exception {
    DFSClient client = Mockito.mock(DFSClient.class);

    BackUpInfo backUpInfo = new BackUpInfo(1L, "/file", "remote/dest/", 10);
    metaStore.insertBackUpInfo(backUpInfo);
    InotifyEventApplier applier = new InotifyEventApplier(metaStore, client);

    Event.CreateEvent createEvent =
        new Event.CreateEvent.Builder()
            .iNodeType(Event.CreateEvent.INodeType.FILE)
            .ctime(1)
            .defaultBlockSize(1024)
            .groupName("cg1")
            .overwrite(true)
            .ownerName("user1")
            .path("/file")
            .perms(new FsPermission("777"))
            .replication(3)
            .build();
    HdfsFileStatus status1 =
        new HdfsFileStatus(
            0,
            false,
            1,
            1024,
            0,
            0,
            new FsPermission("777"),
            "owner",
            "group",
            new byte[0],
            new byte[0],
            1010,
            0,
            null,
            (byte) 0);
    Mockito.when(client.getFileInfo(Matchers.startsWith("/file"))).thenReturn(status1);
    Mockito.when(client.getFileInfo(Matchers.startsWith("/dir"))).thenReturn(getDummyDirStatus("", 1010));
    applier.apply(new Event[] {createEvent});

    FileInfo result1 = metaStore.getFile().get(0);
    Assert.assertEquals(result1.getPath(), "/file");
    Assert.assertEquals(result1.getFileId(), 1010L);
    Assert.assertEquals(result1.getPermission(), 511);

    Event close = new Event.CloseEvent("/file", 1024, 0);
    applier.apply(new Event[] {close});
    FileInfo result2 = metaStore.getFile().get(0);
    Assert.assertEquals(result2.getLength(), 1024);
    Assert.assertEquals(result2.getModificationTime(), 0L);

//    Event truncate = new Event.TruncateEvent("/file", 512, 16);
//    applier.apply(new Event[] {truncate});
//    ResultSet result3 = metaStore.executeQuery("SELECT * FROM files");
//    Assert.assertEquals(result3.getLong("length"), 512);
//    Assert.assertEquals(result3.getLong("modification_time"), 16L);

    Event meta =
        new Event.MetadataUpdateEvent.Builder()
            .path("/file")
            .metadataType(Event.MetadataUpdateEvent.MetadataType.TIMES)
            .mtime(2)
            .atime(3)
            .replication(4)
            .ownerName("user2")
            .groupName("cg2")
            .build();
    applier.apply(new Event[] {meta});
    FileInfo result4 = metaStore.getFile().get(0);
    Assert.assertEquals(result4.getAccessTime(), 3);
    Assert.assertEquals(result4.getModificationTime(), 2);

    Event.CreateEvent createEvent2 =
        new Event.CreateEvent.Builder()
            .iNodeType(Event.CreateEvent.INodeType.DIRECTORY)
            .ctime(1)
            .groupName("cg1")
            .overwrite(true)
            .ownerName("user1")
            .path("/dir")
            .perms(new FsPermission("777"))
            .replication(3)
            .build();
    Event.CreateEvent createEvent3 =
        new Event.CreateEvent.Builder()
            .iNodeType(Event.CreateEvent.INodeType.FILE)
            .ctime(1)
            .groupName("cg1")
            .overwrite(true)
            .ownerName("user1")
            .path("/dir/file")
            .perms(new FsPermission("777"))
            .replication(3)
            .build();
    Event rename =
      new Event.RenameEvent.Builder().dstPath("/dir2").srcPath("/dir").timestamp(5).build();

    applier.apply(new Event[] {createEvent2, createEvent3, rename});
    List<FileInfo> result5 = metaStore.getFile();
    List<String> expectedPaths = Arrays.asList("/dir2", "/dir2/file", "/file");
    List<String> actualPaths = new ArrayList<>();
    for (FileInfo s : result5) {
      actualPaths.add(s.getPath());
    }
    Collections.sort(actualPaths);
    Assert.assertTrue(actualPaths.size() == 3);
    Assert.assertTrue(actualPaths.containsAll(expectedPaths));

    Event unlink = new Event.UnlinkEvent.Builder().path("/").timestamp(6).build();
    applier.apply(new Event[]{unlink});
    Assert.assertFalse(metaStore.getFile().size() > 0);

    List<FileDiff> fileDiffList = metaStore.getPendingDiff();
    Assert.assertTrue(fileDiffList.size() == 3);
  }

  @Test
  public void testApplierCreateEvent() throws Exception {
    DFSClient client = Mockito.mock(DFSClient.class);
    InotifyEventApplier applier = new InotifyEventApplier(metaStore, client);

    BackUpInfo backUpInfo = new BackUpInfo(1L, "/file1", "remote/dest/", 10);
    metaStore.insertBackUpInfo(backUpInfo);

    HdfsFileStatus status1 =
        new HdfsFileStatus(
            0,
            false,
            2,
            123,
            0,
            0,
            new FsPermission("777"),
            "test",
            "group",
            new byte[0],
            new byte[0],
            1010,
            0,
            null,
            (byte) 0);
    Mockito.when(client.getFileInfo("/file1")).thenReturn(status1);

    List<Event> events = new ArrayList<>();
    Event.CreateEvent createEvent = new Event.CreateEvent.Builder().path("/file1").defaultBlockSize(123).ownerName("test")
        .replication(2).perms(new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE)).build();
    events.add(createEvent);
    Mockito.when(client.getFileInfo("/file1")).thenReturn(status1);
    applier.apply(events);

    Assert.assertTrue(metaStore.getFile("/file1").getOwner().equals("test"));
    //judge file diff
    List<FileDiff> fileDiffs = metaStore.getFileDiffsByFileName("/file1");

    Assert.assertTrue(fileDiffs.size() > 0);
    for (FileDiff fileDiff : fileDiffs) {
      if (fileDiff.getDiffType().equals(FileDiffType.APPEND)) {
        //find create diff and compare
        Assert.assertTrue(fileDiff.getParameters().get("-owner").equals("test"));
      }
    }
  }

  @Test
  public void testApplierRenameEvent() throws Exception {
    DFSClient client = Mockito.mock(DFSClient.class);
    InotifyEventApplier applier = new InotifyEventApplier(metaStore, client);

    FileInfo[] fileInfos = new FileInfo[]{
        HadoopUtil.convertFileStatus(getDummyFileStatus("/dirfile", 7000), "/dirfile"),
        HadoopUtil.convertFileStatus(getDummyDirStatus("/dir", 8000), "/dir"),
        HadoopUtil.convertFileStatus(getDummyFileStatus("/dir/file1", 8001), "/dir/file1"),
        HadoopUtil.convertFileStatus(getDummyFileStatus("/dir/file2", 8002), "/dir/file2"),
        HadoopUtil.convertFileStatus(getDummyDirStatus("/dir2", 8100), "/dir2"),
        HadoopUtil.convertFileStatus(getDummyFileStatus("/dir2/file1", 8101), "/dir2/file1"),
        HadoopUtil.convertFileStatus(getDummyFileStatus("/dir2/file2", 8102), "/dir2/file2"),
    };
    metaStore.insertFiles(fileInfos);
    Mockito.when(client.getFileInfo("/dir1")).thenReturn(getDummyDirStatus("/dir1", 8000));
    Event.RenameEvent dirRenameEvent = new Event.RenameEvent.Builder()
        .srcPath("/dir")
        .dstPath("/dir1")
        .build();
    applier.apply(new Event[] {dirRenameEvent});
    Assert.assertTrue(metaStore.getFile("/dir") == null);
    Assert.assertTrue(metaStore.getFile("/dir/file1") == null);
    Assert.assertTrue(metaStore.getFile("/dirfile") != null);
    Assert.assertTrue(metaStore.getFile("/dir1") != null);
    Assert.assertTrue(metaStore.getFile("/dir1/file1") != null);
    Assert.assertTrue(metaStore.getFile("/dir2") != null);
    Assert.assertTrue(metaStore.getFile("/dir2/file1") != null);

    List<Event> events = new ArrayList<>();
    Event.RenameEvent renameEvent = new Event.RenameEvent.Builder()
        .srcPath("/file1")
        .dstPath("/file2")
        .build();
    events.add(renameEvent);
    applier.apply(events);
    Assert.assertTrue(metaStore.getFile("/file2") == null);

    Mockito.when(client.getFileInfo("/file2")).thenReturn(getDummyFileStatus("/file2", 2000));
    applier.apply(events);
    FileInfo info = metaStore.getFile("/file2");
    Assert.assertTrue(info != null && info.getFileId() == 2000);

    events.clear();
    renameEvent = new Event.RenameEvent.Builder()
        .srcPath("/file2")
        .dstPath("/file3")
        .build();
    events.add(renameEvent);
    applier.apply(events);
    FileInfo info2 = metaStore.getFile("/file2");
    Assert.assertTrue(info2 == null);
    FileInfo info3 = metaStore.getFile("/file3");
    Assert.assertTrue(info3 != null);

    renameEvent = new Event.RenameEvent.Builder()
        .srcPath("/file3")
        .dstPath("/file4")
        .build();
    events.clear();
    events.add(renameEvent);
    applier.apply(events);
    FileInfo info4 = metaStore.getFile("/file3");
    FileInfo info5 = metaStore.getFile("/file4");
    Assert.assertTrue(info4 == null && info5 != null);
  }

  private HdfsFileStatus getDummyFileStatus(String file, long fid) {
    return doGetDummyStatus(file, fid, false);
  }

  private HdfsFileStatus getDummyDirStatus(String file, long fid) {
    return doGetDummyStatus(file, fid, true);
  }

  private HdfsFileStatus doGetDummyStatus(String file, long fid, boolean isdir) {
    return new HdfsFileStatus(
        0,
        isdir,
        1,
        1024,
        0,
        0,
        new FsPermission("777"),
        "owner",
        "group",
        new byte[0],
        file.getBytes(),
        fid,
        0,
        null,
        (byte) 0);
  }

  @After
  public void cleanUp() throws Exception {
    closeDao();
  }
}
