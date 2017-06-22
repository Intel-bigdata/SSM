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

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.smartdata.server.engine.MetaStore;
import org.smartdata.server.engine.metastore.MetaUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestInotifyEventApplier extends DBTest {

  @Test
  public void testApplier() throws Exception {
    DFSClient client = Mockito.mock(DFSClient.class);
    Connection connection = databaseTester.getConnection().getConnection();
    MetaUtil.initializeDataBase(connection);
    MetaStore metaStore = new MetaStore(connection);
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
            new FsPermission((short) 777),
            "owner",
            "group",
            new byte[0],
            new byte[0],
            1010,
            0,
            null,
            (byte) 0);
    Mockito.when(client.getFileInfo(Matchers.anyString())).thenReturn(status1);
    applier.apply(new Event[] {createEvent});

    ResultSet result1 = metaStore.executeQuery("SELECT * FROM files");
    Assert.assertEquals(result1.getString("path"), "/file");
    Assert.assertEquals(result1.getLong("fid"), 1010L);
    Assert.assertEquals(result1.getShort("permission"), 511);

    Event close = new Event.CloseEvent("/file", 1024, 0);
    applier.apply(new Event[] {close});
    ResultSet result2 = metaStore.executeQuery("SELECT * FROM files");
    Assert.assertEquals(result2.getLong("length"), 1024);
    Assert.assertEquals(result2.getLong("modification_time"), 0L);

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
    ResultSet result4 = metaStore.executeQuery("SELECT * FROM files");
    Assert.assertEquals(result4.getLong("access_time"), 3);
    Assert.assertEquals(result4.getLong("modification_time"), 2);

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
    ResultSet result5 = metaStore.executeQuery("SELECT * FROM files");
    List<String> expectedPaths = Arrays.asList("/dir2", "/dir2/file", "/file");
    List<String> actualPaths = new ArrayList<>();
    while (result5.next()) {
      actualPaths.add(result5.getString("path"));
    }
    Collections.sort(actualPaths);
    Assert.assertTrue(actualPaths.size() == 3);
    Assert.assertTrue(actualPaths.containsAll(expectedPaths));

    Event unlink = new Event.UnlinkEvent.Builder().path("/").timestamp(6).build();
    applier.apply(new Event[] {unlink});
    ResultSet result6 = metaStore.executeQuery("SELECT * FROM files");
    Assert.assertFalse(result6.next());
  }
}
