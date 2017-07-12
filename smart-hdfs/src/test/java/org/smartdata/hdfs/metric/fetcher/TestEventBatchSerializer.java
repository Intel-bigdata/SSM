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

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestEventBatchSerializer {

  @Test
  public void testSerializer() throws InvalidProtocolBufferException {
    Event close = new Event.CloseEvent("/user1", 1024, 0);
    Event create =
        new Event.CreateEvent.Builder()
            .iNodeType(Event.CreateEvent.INodeType.FILE)
            .ctime(1)
            .defaultBlockSize(1024)
            .groupName("cg1")
            .overwrite(true)
            .ownerName("user1")
            .path("/file1")
            .perms(new FsPermission("777"))
            .replication(3)
            .build();
    Event meta =
        new Event.MetadataUpdateEvent.Builder()
            .path("/file2")
            .metadataType(Event.MetadataUpdateEvent.MetadataType.OWNER)
            .mtime(2)
            .atime(3)
            .replication(4)
            .ownerName("user2")
            .groupName("cg2")
            .build();
    Event rename =
        new Event.RenameEvent.Builder().dstPath("/file4").srcPath("/file3").timestamp(5).build();
    Event append = new Event.AppendEvent.Builder().path("/file5").build();
    Event unlink = new Event.UnlinkEvent.Builder().path("/file6").timestamp(6).build();
//    Event truncate = new Event.TruncateEvent("/file7", 1024, 16);
    List<Event> events = Arrays.asList(close, create, meta, rename, append, unlink);
    EventBatch batch = new EventBatch(1023, events.toArray(new Event[0]));
    List<String> expected = new ArrayList<>();
    for (Event event : events) {
      expected.add(event.toString());
    }

    byte[] bytes = EventBatchSerializer.serialize(batch);
    EventBatch result = EventBatchSerializer.deserialize(bytes);
    List<String> actual = new ArrayList<>();
    for (Event event : result.getEvents()) {
      actual.add(event.toString());
    }
    Assert.assertEquals(batch.getTxid(), result.getTxid());
    Assert.assertEquals(expected.size(), actual.size());
//    Assert.assertTrue(expected.containsAll(actual));
  }
}
