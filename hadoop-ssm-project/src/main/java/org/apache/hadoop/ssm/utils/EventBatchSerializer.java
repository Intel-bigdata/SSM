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
package org.apache.hadoop.ssm.utils;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos;
import org.apache.hadoop.hdfs.protocol.proto.InotifyProtos;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;

import java.util.List;

public class EventBatchSerializer {

  //Code copy from PBHelperClient.java
  public static byte[] serialize(EventBatch eventBatch) {
    List<InotifyProtos.EventProto> events = Lists.newArrayList();
    for (Event e : eventBatch.getEvents()) {
      switch (e.getEventType()) {
        case CLOSE:
          Event.CloseEvent ce = (Event.CloseEvent) e;
          events.add(InotifyProtos.EventProto.newBuilder()
            .setType(InotifyProtos.EventType.EVENT_CLOSE)
            .setContents(
              InotifyProtos.CloseEventProto.newBuilder()
                .setPath(ce.getPath())
                .setFileSize(ce.getFileSize())
                .setTimestamp(ce.getTimestamp()).build().toByteString()
            ).build());
          break;
        case CREATE:
          Event.CreateEvent ce2 = (Event.CreateEvent) e;
          events.add(InotifyProtos.EventProto.newBuilder()
            .setType(InotifyProtos.EventType.EVENT_CREATE)
            .setContents(
              InotifyProtos.CreateEventProto.newBuilder()
                .setType(PBHelperClient.createTypeConvert(ce2.getiNodeType()))
                .setPath(ce2.getPath())
                .setCtime(ce2.getCtime())
                .setOwnerName(ce2.getOwnerName())
                .setGroupName(ce2.getGroupName())
                .setPerms(PBHelperClient.convert(ce2.getPerms()))
                .setReplication(ce2.getReplication())
                .setSymlinkTarget(ce2.getSymlinkTarget() == null ?
                  "" : ce2.getSymlinkTarget())
                .setDefaultBlockSize(ce2.getDefaultBlockSize())
                .setOverwrite(ce2.getOverwrite()).build().toByteString()
            ).build());
          break;
        case METADATA:
          Event.MetadataUpdateEvent me = (Event.MetadataUpdateEvent) e;
          InotifyProtos.MetadataUpdateEventProto.Builder metaB =
            InotifyProtos.MetadataUpdateEventProto.newBuilder()
              .setPath(me.getPath())
              .setType(PBHelperClient.metadataUpdateTypeConvert(me.getMetadataType()))
              .setMtime(me.getMtime())
              .setAtime(me.getAtime())
              .setReplication(me.getReplication())
              .setOwnerName(me.getOwnerName() == null ? "" :
                me.getOwnerName())
              .setGroupName(me.getGroupName() == null ? "" :
                me.getGroupName())
              .addAllAcls(me.getAcls() == null ?
                Lists.<AclProtos.AclEntryProto>newArrayList() :
                PBHelperClient.convertAclEntryProto(me.getAcls()))
              .addAllXAttrs(me.getxAttrs() == null ?
                Lists.<XAttrProtos.XAttrProto>newArrayList() :
                PBHelperClient.convertXAttrProto(me.getxAttrs()))
              .setXAttrsRemoved(me.isxAttrsRemoved());
          if (me.getPerms() != null) {
            metaB.setPerms(PBHelperClient.convert(me.getPerms()));
          }
          events.add(InotifyProtos.EventProto.newBuilder()
            .setType(InotifyProtos.EventType.EVENT_METADATA)
            .setContents(metaB.build().toByteString())
            .build());
          break;
        case RENAME:
          Event.RenameEvent re = (Event.RenameEvent) e;
          events.add(InotifyProtos.EventProto.newBuilder()
            .setType(InotifyProtos.EventType.EVENT_RENAME)
            .setContents(
              InotifyProtos.RenameEventProto.newBuilder()
                .setSrcPath(re.getSrcPath())
                .setDestPath(re.getDstPath())
                .setTimestamp(re.getTimestamp()).build().toByteString()
            ).build());
          break;
        case APPEND:
          Event.AppendEvent re2 = (Event.AppendEvent) e;
          events.add(InotifyProtos.EventProto.newBuilder()
            .setType(InotifyProtos.EventType.EVENT_APPEND)
            .setContents(InotifyProtos.AppendEventProto.newBuilder()
              .setPath(re2.getPath())
              .setNewBlock(re2.toNewBlock()).build().toByteString())
            .build());
          break;
        case UNLINK:
          Event.UnlinkEvent ue = (Event.UnlinkEvent) e;
          events.add(InotifyProtos.EventProto.newBuilder()
            .setType(InotifyProtos.EventType.EVENT_UNLINK)
            .setContents(
              InotifyProtos.UnlinkEventProto.newBuilder()
                .setPath(ue.getPath())
                .setTimestamp(ue.getTimestamp()).build().toByteString()
            ).build());
          break;
        case TRUNCATE:
          Event.TruncateEvent te = (Event.TruncateEvent) e;
          events.add(InotifyProtos.EventProto.newBuilder()
            .setType(InotifyProtos.EventType.EVENT_TRUNCATE)
            .setContents(
              InotifyProtos.TruncateEventProto.newBuilder()
                .setPath(te.getPath())
                .setFileSize(te.getFileSize())
                .setTimestamp(te.getTimestamp()).build().toByteString()
            ).build());
          break;
        default:
          throw new RuntimeException("Unexpected inotify event: " + e);
      }
    }
    return InotifyProtos.EventBatchProto.newBuilder().
      setTxid(eventBatch.getTxid()).
      addAllEvents(events).build().toByteArray();
  }

  public static EventBatch deserialize(byte[] bytes) throws InvalidProtocolBufferException {
    InotifyProtos.EventBatchProto proto = InotifyProtos.EventBatchProto.parseFrom(bytes);
    long txid = proto.getTxid();
    List<Event> events = Lists.newArrayList();
    for (InotifyProtos.EventProto p : proto.getEventsList()) {
      switch (p.getType()) {
        case EVENT_CLOSE:
          InotifyProtos.CloseEventProto close =
            InotifyProtos.CloseEventProto.parseFrom(p.getContents());
          events.add(new Event.CloseEvent(close.getPath(),
            close.getFileSize(), close.getTimestamp()));
          break;
        case EVENT_CREATE:
          InotifyProtos.CreateEventProto create =
            InotifyProtos.CreateEventProto.parseFrom(p.getContents());
          events.add(new Event.CreateEvent.Builder()
            .iNodeType(PBHelperClient.createTypeConvert(create.getType()))
            .path(create.getPath())
            .ctime(create.getCtime())
            .ownerName(create.getOwnerName())
            .groupName(create.getGroupName())
            .perms(PBHelperClient.convert(create.getPerms()))
            .replication(create.getReplication())
            .symlinkTarget(create.getSymlinkTarget().isEmpty() ? null :
              create.getSymlinkTarget())
            .defaultBlockSize(create.getDefaultBlockSize())
            .overwrite(create.getOverwrite()).build());
          break;
        case EVENT_METADATA:
          InotifyProtos.MetadataUpdateEventProto meta =
            InotifyProtos.MetadataUpdateEventProto.parseFrom(p.getContents());
          events.add(new Event.MetadataUpdateEvent.Builder()
            .path(meta.getPath())
            .metadataType(PBHelperClient.metadataUpdateTypeConvert(meta.getType()))
            .mtime(meta.getMtime())
            .atime(meta.getAtime())
            .replication(meta.getReplication())
            .ownerName(
              meta.getOwnerName().isEmpty() ? null : meta.getOwnerName())
            .groupName(
              meta.getGroupName().isEmpty() ? null : meta.getGroupName())
            .perms(meta.hasPerms() ? PBHelperClient.convert(meta.getPerms()) : null)
            .acls(meta.getAclsList().isEmpty() ? null : PBHelperClient.convertAclEntry(
              meta.getAclsList()))
            .xAttrs(meta.getXAttrsList().isEmpty() ? null : PBHelperClient.convertXAttrs(
              meta.getXAttrsList()))
            .xAttrsRemoved(meta.getXAttrsRemoved())
            .build());
          break;
        case EVENT_RENAME:
          InotifyProtos.RenameEventProto rename =
            InotifyProtos.RenameEventProto.parseFrom(p.getContents());
          events.add(new Event.RenameEvent.Builder()
            .srcPath(rename.getSrcPath())
            .dstPath(rename.getDestPath())
            .timestamp(rename.getTimestamp())
            .build());
          break;
        case EVENT_APPEND:
          InotifyProtos.AppendEventProto append =
            InotifyProtos.AppendEventProto.parseFrom(p.getContents());
          events.add(new Event.AppendEvent.Builder().path(append.getPath())
            .newBlock(append.hasNewBlock() && append.getNewBlock())
            .build());
          break;
        case EVENT_UNLINK:
          InotifyProtos.UnlinkEventProto unlink =
            InotifyProtos.UnlinkEventProto.parseFrom(p.getContents());
          events.add(new Event.UnlinkEvent.Builder()
            .path(unlink.getPath())
            .timestamp(unlink.getTimestamp())
            .build());
          break;
        case EVENT_TRUNCATE:
          InotifyProtos.TruncateEventProto truncate =
            InotifyProtos.TruncateEventProto.parseFrom(p.getContents());
          events.add(new Event.TruncateEvent(truncate.getPath(),
            truncate.getFileSize(), truncate.getTimestamp()));
          break;
        default:
          throw new RuntimeException("Unexpected inotify event type: " +
            p.getType());
      }
    }
    return new EventBatch(txid, events.toArray(new Event[events.size()]));
  }
}
