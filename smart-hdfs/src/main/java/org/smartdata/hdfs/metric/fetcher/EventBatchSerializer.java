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

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.protocol.FsPermissionExtension;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.InotifyProtos;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.FsPermissionProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.AclEntryProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.XAttrProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.XAttrProto.XAttrNamespaceProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.AclEntryProto.AclEntryTypeProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.AclEntryProto.AclEntryScopeProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.AclEntryProto.FsActionProto;
import java.util.ArrayList;
import java.util.List;

public class EventBatchSerializer {
  private static final AclEntryScope[] ACL_ENTRY_SCOPE_VALUES =
    AclEntryScope.values();
  private static final AclEntryType[] ACL_ENTRY_TYPE_VALUES =
    AclEntryType.values();
  private static final FsAction[] FSACTION_VALUES =
    FsAction.values();
  private static final XAttr.NameSpace[] XATTR_NAMESPACE_VALUES =
    XAttr.NameSpace.values();

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
                .setType(createTypeConvert(ce2.getiNodeType()))
                .setPath(ce2.getPath())
                .setCtime(ce2.getCtime())
                .setOwnerName(ce2.getOwnerName())
                .setGroupName(ce2.getGroupName())
                .setPerms(convert(ce2.getPerms()))
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
              .setType(metadataUpdateTypeConvert(me.getMetadataType()))
              .setMtime(me.getMtime())
              .setAtime(me.getAtime())
              .setReplication(me.getReplication())
              .setOwnerName(me.getOwnerName() == null ? "" :
                me.getOwnerName())
              .setGroupName(me.getGroupName() == null ? "" :
                me.getGroupName())
              .addAllAcls(me.getAcls() == null ?
                Lists.<AclProtos.AclEntryProto>newArrayList() :
                convertAclEntryProto(me.getAcls()))
              .addAllXAttrs(me.getxAttrs() == null ?
                Lists.<XAttrProtos.XAttrProto>newArrayList() :
                convertXAttrProto(me.getxAttrs()))
              .setXAttrsRemoved(me.isxAttrsRemoved());
          if (me.getPerms() != null) {
            metaB.setPerms(convert(me.getPerms()));
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
              .build().toByteString())
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
        /*
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
          */
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
            .iNodeType(createTypeConvert(create.getType()))
            .path(create.getPath())
            .ctime(create.getCtime())
            .ownerName(create.getOwnerName())
            .groupName(create.getGroupName())
            .perms(convert(create.getPerms()))
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
            .metadataType(metadataUpdateTypeConvert(meta.getType()))
            .mtime(meta.getMtime())
            .atime(meta.getAtime())
            .replication(meta.getReplication())
            .ownerName(
              meta.getOwnerName().isEmpty() ? null : meta.getOwnerName())
            .groupName(
              meta.getGroupName().isEmpty() ? null : meta.getGroupName())
            .perms(meta.hasPerms() ? convert(meta.getPerms()) : null)
            .acls(meta.getAclsList().isEmpty() ? null : convertAclEntry(
              meta.getAclsList()))
            .xAttrs(meta.getXAttrsList().isEmpty() ? null : convertXAttrs(
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
        /*
        case EVENT_TRUNCATE:
          InotifyProtos.TruncateEventProto truncate =
            InotifyProtos.TruncateEventProto.parseFrom(p.getContents());
          events.add(new Event.TruncateEvent(truncate.getPath(),
            truncate.getFileSize(), truncate.getTimestamp()));
          break;
          */
        default:
          throw new RuntimeException("Unexpected inotify event type: " +
            p.getType());
      }
    }
    return new EventBatch(txid, events.toArray(new Event[events.size()]));
  }

  private static InotifyProtos.INodeType createTypeConvert(Event.CreateEvent.INodeType
      type) {
    switch (type) {
      case DIRECTORY:
        return InotifyProtos.INodeType.I_TYPE_DIRECTORY;
      case FILE:
        return InotifyProtos.INodeType.I_TYPE_FILE;
      case SYMLINK:
        return InotifyProtos.INodeType.I_TYPE_SYMLINK;
      default:
        return null;
    }
  }

  private static Event.CreateEvent.INodeType createTypeConvert(InotifyProtos.INodeType
    type) {
    switch (type) {
      case I_TYPE_DIRECTORY:
        return Event.CreateEvent.INodeType.DIRECTORY;
      case I_TYPE_FILE:
        return Event.CreateEvent.INodeType.FILE;
      case I_TYPE_SYMLINK:
        return Event.CreateEvent.INodeType.SYMLINK;
      default:
        return null;
    }
  }

  public static HdfsProtos.FsPermissionProto convert(FsPermission p) {
    return FsPermissionProto.newBuilder().setPerm(p.toExtendedShort()).build();
  }

  public static FsPermission convert(FsPermissionProto p) {
    return new FsPermissionExtension((short)p.getPerm());
  }

  private static InotifyProtos.MetadataUpdateType metadataUpdateTypeConvert(
    Event.MetadataUpdateEvent.MetadataType type) {
    switch (type) {
      case TIMES:
        return InotifyProtos.MetadataUpdateType.META_TYPE_TIMES;
      case REPLICATION:
        return InotifyProtos.MetadataUpdateType.META_TYPE_REPLICATION;
      case OWNER:
        return InotifyProtos.MetadataUpdateType.META_TYPE_OWNER;
      case PERMS:
        return InotifyProtos.MetadataUpdateType.META_TYPE_PERMS;
      case ACLS:
        return InotifyProtos.MetadataUpdateType.META_TYPE_ACLS;
      case XATTRS:
        return InotifyProtos.MetadataUpdateType.META_TYPE_XATTRS;
      default:
        return null;
    }
  }

  private static Event.MetadataUpdateEvent.MetadataType metadataUpdateTypeConvert(
    InotifyProtos.MetadataUpdateType type) {
    switch (type) {
      case META_TYPE_TIMES:
        return Event.MetadataUpdateEvent.MetadataType.TIMES;
      case META_TYPE_REPLICATION:
        return Event.MetadataUpdateEvent.MetadataType.REPLICATION;
      case META_TYPE_OWNER:
        return Event.MetadataUpdateEvent.MetadataType.OWNER;
      case META_TYPE_PERMS:
        return Event.MetadataUpdateEvent.MetadataType.PERMS;
      case META_TYPE_ACLS:
        return Event.MetadataUpdateEvent.MetadataType.ACLS;
      case META_TYPE_XATTRS:
        return Event.MetadataUpdateEvent.MetadataType.XATTRS;
      default:
        return null;
    }
  }

  public static List<AclEntryProto> convertAclEntryProto(
      List<AclEntry> aclSpec) {
    ArrayList<AclEntryProto> r = Lists.newArrayListWithCapacity(aclSpec.size());
    for (AclEntry e : aclSpec) {
      AclEntryProto.Builder builder = AclEntryProto.newBuilder();
      builder.setType(convert(e.getType()));
      builder.setScope(convert(e.getScope()));
      builder.setPermissions(convert(e.getPermission()));
      if (e.getName() != null) {
        builder.setName(e.getName());
      }
      r.add(builder.build());
    }
    return r;
  }

  private static XAttrNamespaceProto convert(XAttr.NameSpace v) {
    return XAttrNamespaceProto.valueOf(v.ordinal());
  }

  private static XAttr.NameSpace convert(XAttrNamespaceProto v) {
    return castEnum(v, XATTR_NAMESPACE_VALUES);
  }

  public static List<AclEntry> convertAclEntry(List<AclEntryProto> aclSpec) {
    ArrayList<AclEntry> r = Lists.newArrayListWithCapacity(aclSpec.size());
    for (AclEntryProto e : aclSpec) {
      AclEntry.Builder builder = new AclEntry.Builder();
      builder.setType(convert(e.getType()));
      builder.setScope(convert(e.getScope()));
      builder.setPermission(convert(e.getPermissions()));
      if (e.hasName()) {
        builder.setName(e.getName());
      }
      r.add(builder.build());
    }
    return r;
  }

  public static List<XAttr> convertXAttrs(List<XAttrProto> xAttrSpec) {
    ArrayList<XAttr> xAttrs = Lists.newArrayListWithCapacity(xAttrSpec.size());
    for (XAttrProto a : xAttrSpec) {
      XAttr.Builder builder = new XAttr.Builder();
      builder.setNameSpace(convert(a.getNamespace()));
      if (a.hasName()) {
        builder.setName(a.getName());
      }
      if (a.hasValue()) {
        builder.setValue(a.getValue().toByteArray());
      }
      xAttrs.add(builder.build());
    }
    return xAttrs;
  }

  public static List<XAttrProto> convertXAttrProto(
      List<XAttr> xAttrSpec) {
    if (xAttrSpec == null) {
      return Lists.newArrayListWithCapacity(0);
    }
    ArrayList<XAttrProto> xAttrs = Lists.newArrayListWithCapacity(
      xAttrSpec.size());
    for (XAttr a : xAttrSpec) {
      XAttrProto.Builder builder = XAttrProto.newBuilder();
      builder.setNamespace(convert(a.getNameSpace()));
      if (a.getName() != null) {
        builder.setName(a.getName());
      }
      if (a.getValue() != null) {
        builder.setValue(getByteString(a.getValue()));
      }
      xAttrs.add(builder.build());
    }
    return xAttrs;
  }

  public static XAttrProto convertXAttrProto(XAttr a) {
    XAttrProto.Builder builder = XAttrProto.newBuilder();
    builder.setNamespace(convert(a.getNameSpace()));
    if (a.getName() != null) {
      builder.setName(a.getName());
    }
    if (a.getValue() != null) {
      builder.setValue(getByteString(a.getValue()));
    }
    return builder.build();
  }

  public static ByteString getByteString(byte[] bytes) {
    return ByteString.copyFrom(bytes);
  }

  private static AclEntryTypeProto convert(AclEntryType e) {
    return AclEntryTypeProto.valueOf(e.ordinal());
  }

  private static AclEntryScopeProto convert(AclEntryScope v) {
    return AclEntryScopeProto.valueOf(v.ordinal());
  }

  public static FsActionProto convert(FsAction v) {
    return FsActionProto.valueOf(v != null ? v.ordinal() : 0);
  }

  public static FsAction convert(FsActionProto v) {
    return castEnum(v, FSACTION_VALUES);
  }

  private static <T extends Enum<T>, U extends Enum<U>> U castEnum(T from, U[] to) {
    return to[from.ordinal()];
  }

  private static AclEntryType convert(AclEntryTypeProto v) {
    return castEnum(v, ACL_ENTRY_TYPE_VALUES);
  }

  private static AclEntryScope convert(AclEntryScopeProto v) {
    return castEnum(v, ACL_ENTRY_SCOPE_VALUES);
  }

}
