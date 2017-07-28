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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.inotify.Event;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.smartdata.metastore.MetaStore;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Executors;

public class TestInotifyFetcher {
  private static final int BLOCK_SIZE = 1024;

  private static class EventApplierForTest extends InotifyEventApplier {
    private List<Event> events = new ArrayList<>();

    public EventApplierForTest(MetaStore metaStore, DFSClient client) {
      super(metaStore, client);
    }

    @Override
    public void apply(Event[] evs) {
      events.addAll(Arrays.asList(evs));
    }

    public List<Event> getEvents() {
      return events;
    }
  }

  @Test(timeout = 60000)
  public void testFetcher() throws IOException, InterruptedException {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    // so that we can get an atime change
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 1);

    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    builder.numDataNodes(2);
    MiniDFSCluster cluster = builder.build();
    try {
      cluster.waitActive();
      DFSClient client = new DFSClient(cluster.getNameNode(0)
        .getNameNodeAddress(), conf);

      FileSystem fs = cluster.getFileSystem(0);
      DFSTestUtil.createFile(fs, new Path("/file"), BLOCK_SIZE, (short) 1, 0L);
      DFSTestUtil.createFile(fs, new Path("/file3"), BLOCK_SIZE, (short) 1, 0L);
      DFSTestUtil.createFile(fs, new Path("/file5"), BLOCK_SIZE, (short) 1, 0L);
      DFSTestUtil.createFile(fs, new Path("/truncate_file"),
        BLOCK_SIZE * 2, (short) 1, 0L);
      fs.mkdirs(new Path("/tmp"), new FsPermission("777"));

      MetaStore metaStore = Mockito.mock(MetaStore.class);
      EventApplierForTest applierForTest = new EventApplierForTest(metaStore, client);
      final InotifyEventFetcher fetcher = new InotifyEventFetcher(client, metaStore,
          Executors.newScheduledThreadPool(2), applierForTest);

      Thread thread = new Thread() {
        public void run() {
          try {
            fetcher.start();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      };
      thread.start();

      Thread.sleep(2000);

      /**
       * Code copy from {@link org.apache.hadoop.hdfs.TestDFSInotifyEventInputStream}
       */
      client.rename("/file", "/file4", null); // RenameOp -> RenameEvent
      client.rename("/file4", "/file2"); // RenameOldOp -> RenameEvent
      // DeleteOp, AddOp -> UnlinkEvent, CreateEvent
      OutputStream os = client.create("/file2", true, (short) 2, BLOCK_SIZE);
      os.write(new byte[BLOCK_SIZE]);
      os.close(); // CloseOp -> CloseEvent
      // AddOp -> AppendEvent
      os = client.append("/file2", BLOCK_SIZE, null, null);
      os.write(new byte[BLOCK_SIZE]);
      os.close(); // CloseOp -> CloseEvent
      Thread.sleep(10); // so that the atime will get updated on the next line
      client.open("/file2").read(new byte[1]); // TimesOp -> MetadataUpdateEvent
      // SetReplicationOp -> MetadataUpdateEvent
      client.setReplication("/file2", (short) 1);
      // ConcatDeleteOp -> AppendEvent, UnlinkEvent, CloseEvent
      client.concat("/file2", new String[]{"/file3"});
      client.delete("/file2", false); // DeleteOp -> UnlinkEvent
      client.mkdirs("/dir", null, false); // MkdirOp -> CreateEvent
      // SetPermissionsOp -> MetadataUpdateEvent
      client.setPermission("/dir", FsPermission.valueOf("-rw-rw-rw-"));
      // SetOwnerOp -> MetadataUpdateEvent

      Thread.sleep(2000);

      client.setOwner("/dir", "username", "groupname");
      client.createSymlink("/dir", "/dir2", false); // SymlinkOp -> CreateEvent
      client.setXAttr("/file5", "user.field", "value".getBytes(), EnumSet.of(
        XAttrSetFlag.CREATE)); // SetXAttrOp -> MetadataUpdateEvent
      // RemoveXAttrOp -> MetadataUpdateEvent
      client.removeXAttr("/file5", "user.field");
      // SetAclOp -> MetadataUpdateEvent
      client.setAcl("/file5", AclEntry.parseAclSpec(
        "user::rwx,user:foo:rw-,group::r--,other::---", true));
      client.removeAcl("/file5"); // SetAclOp -> MetadataUpdateEvent
      client.rename("/file5", "/dir"); // RenameOldOp -> RenameEvent

      while (applierForTest.getEvents().size() != 21) {
        Thread.sleep(100);
      }

      /**
       * Refer {@link org.apache.hadoop.hdfs.TestDFSInotifyEventInputStream} for more detail
       */
      List<Event> events = applierForTest.getEvents();
      Assert.assertTrue(events.get(0).getEventType() == Event.EventType.RENAME);
      Assert.assertTrue(events.get(1).getEventType() == Event.EventType.RENAME);
      Assert.assertTrue(events.get(2).getEventType() == Event.EventType.CREATE);
      Assert.assertTrue(events.get(3).getEventType() == Event.EventType.CLOSE);
      Assert.assertTrue(events.get(4).getEventType() == Event.EventType.APPEND);
      Assert.assertTrue(events.get(5).getEventType() == Event.EventType.CLOSE);
      Assert.assertTrue(events.get(6).getEventType() == Event.EventType.METADATA);
      Assert.assertTrue(events.get(7).getEventType() == Event.EventType.METADATA);
      Assert.assertTrue(events.get(8).getEventType() == Event.EventType.APPEND);
      Assert.assertTrue(events.get(9).getEventType() == Event.EventType.UNLINK);
      Assert.assertTrue(events.get(10).getEventType() == Event.EventType.CLOSE);
      Assert.assertTrue(events.get(11).getEventType() == Event.EventType.UNLINK);
      Assert.assertTrue(events.get(12).getEventType() == Event.EventType.CREATE);
      Assert.assertTrue(events.get(13).getEventType() == Event.EventType.METADATA);
      Assert.assertTrue(events.get(14).getEventType() == Event.EventType.METADATA);
      Assert.assertTrue(events.get(15).getEventType() == Event.EventType.CREATE);
      Assert.assertTrue(events.get(16).getEventType() == Event.EventType.METADATA);
      Assert.assertTrue(events.get(17).getEventType() == Event.EventType.METADATA);
      Assert.assertTrue(events.get(18).getEventType() == Event.EventType.METADATA);
      Assert.assertTrue(events.get(19).getEventType() == Event.EventType.METADATA);
      Assert.assertTrue(events.get(20).getEventType() == Event.EventType.RENAME);
//      Assert.assertTrue(events.get(21).getEventType() == Event.EventType.TRUNCATE);
      fetcher.stop();
    } finally {
      cluster.shutdown();
    }
  }
}
