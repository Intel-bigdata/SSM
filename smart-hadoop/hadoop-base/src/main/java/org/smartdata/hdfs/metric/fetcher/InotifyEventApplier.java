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

import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.FileDiff;
import org.smartdata.model.FileDiffType;
import org.smartdata.model.FileInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This is a very preliminary and buggy applier, can further enhance by referring to
 * {@link org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader}
 */
public class InotifyEventApplier {
  private final MetaStore metaStore;
  private DFSClient client;
  private static final Logger LOG =
      LoggerFactory.getLogger(InotifyEventFetcher.class);

  public InotifyEventApplier(MetaStore metaStore, DFSClient client) {
    this.metaStore = metaStore;
    this.client = client;
  }

  public void apply(List<Event> events) throws IOException, MetaStoreException {
    List<String> statements = new ArrayList<>();
    for (Event event : events) {
      List<String> gen = getSqlStatement(event);
      if (gen != null && !gen.isEmpty()){
        for (String s : gen) {
          if (s != null && s.length() > 0) {
            statements.add(s);
          }
        }
      }
    }
    this.metaStore.execute(statements);
  }

  public void apply(Event[] events) throws IOException, MetaStoreException {
    this.apply(Arrays.asList(events));
  }

  private List<String> getSqlStatement(Event event) throws IOException, MetaStoreException {
    FileDiff fileDiff = new FileDiff();
    fileDiff.setCreate_time(System.currentTimeMillis());
    fileDiff.setApplied(false);
    switch (event.getEventType()) {
      // TODO parse and save to fileDiff
      case CREATE:
        LOG.trace("event type:" + event.getEventType().name() +
            ", path:" + ((Event.CreateEvent)event).getPath());
        /*fileDiff.setDiffType(FileDiffType.CREATE);
        fileDiff.setParameters(String.format("-file %s",
            ((Event.CreateEvent)event).getPath()));
        metaStore.insertFileDiff(fileDiff);*/
        return Arrays.asList(this.getCreateSql((Event.CreateEvent)event));
      case CLOSE:
        LOG.trace("event type:" + event.getEventType().name() +
            ", path:" + ((Event.CloseEvent)event).getPath());
        fileDiff.setDiffType(FileDiffType.APPEND);
        fileDiff.setParameters(String.format("-file %s -length %s",
            ((Event.CloseEvent)event).getPath(),
            ((Event.CloseEvent)event).getFileSize()));
        metaStore.insertFileDiff(fileDiff);
        return Arrays.asList(this.getCloseSql((Event.CloseEvent)event));
      case RENAME:
        LOG.trace("event type:" + event.getEventType().name() +
            ", src path:" + ((Event.RenameEvent)event).getSrcPath() +
            ", dest path:" + ((Event.RenameEvent)event).getDstPath());
        fileDiff.setDiffType(FileDiffType.RENAME);
        fileDiff.setParameters(String.format("-file %s -dest %s",
            ((Event.RenameEvent)event).getSrcPath(),
            ((Event.RenameEvent)event).getDstPath()));
        metaStore.insertFileDiff(fileDiff);
        return this.getRenameSql((Event.RenameEvent)event);
      case METADATA:
        LOG.trace("event type:" + event.getEventType().name() +
            ", path:" + ((Event.MetadataUpdateEvent)event).getPath());
        return Arrays.asList(this.getMetaDataUpdateSql((Event.MetadataUpdateEvent)event));
      case APPEND:
        LOG.trace("event type:" + event.getEventType().name() +
            ", path:" + ((Event.AppendEvent)event).getPath());
        return this.getAppendSql((Event.AppendEvent)event);
      case UNLINK:
        LOG.trace("event type:" + event.getEventType().name() +
            ", path:" + ((Event.UnlinkEvent)event).getPath());
        fileDiff.setDiffType(FileDiffType.DELETE);
        fileDiff.setParameters(String.format("-file %s",
            ((Event.UnlinkEvent)event).getPath()));
        metaStore.insertFileDiff(fileDiff);
        return this.getUnlinkSql((Event.UnlinkEvent)event);
    }
    return Arrays.asList();
  }

  //Todo: times and ec policy id, etc.
  private String getCreateSql(Event.CreateEvent createEvent) throws IOException {
    HdfsFileStatus fileStatus = client.getFileInfo(createEvent.getPath());
    if (fileStatus == null) {
      LOG.debug("Can not get HdfsFileStatus for file " + createEvent.getPath());
      return "";
    }
    FileInfo fileInfo = HadoopUtil.convertFileStatus(fileStatus, createEvent.getPath());
    try {
      metaStore.insertFile(fileInfo);
      return "";
    } catch (MetaStoreException e) {
      LOG.error("Insert new created file " + fileInfo.getPath() + " error.", e);
      throw new IOException(e);
    }
  }

  //Todo: should update mtime? atime?
  private String getCloseSql(Event.CloseEvent closeEvent) {
    return String.format(
        "UPDATE files SET length = %s, modification_time = %s WHERE path = '%s';",
        closeEvent.getFileSize(), closeEvent.getTimestamp(), closeEvent.getPath());
  }

  //Todo: should update mtime? atime?
//  private String getTruncateSql(Event.TruncateEvent truncateEvent) {
//    return String.format(
//        "UPDATE files SET length = %s, modification_time = %s WHERE path = '%s';",
//        truncateEvent.getFileSize(), truncateEvent.getTimestamp(), truncateEvent.getPath());
//  }

  private List<String> getRenameSql(Event.RenameEvent renameEvent)
      throws IOException, MetaStoreException {
    List<String> ret = new ArrayList<>();
    HdfsFileStatus status = client.getFileInfo(renameEvent.getDstPath());
    if (status == null) {
      LOG.debug("Get rename dest status failed, {} -> {}",
          renameEvent.getSrcPath(), renameEvent.getDstPath());
    }

    FileInfo info = metaStore.getFile(renameEvent.getSrcPath());
    if (info == null) {
      if (status != null) {
        info = HadoopUtil.convertFileStatus(status, renameEvent.getDstPath());
        metaStore.insertFile(info);
      }
    } else {
      ret.add(String.format("UPDATE files SET path = replace(path, '%s', '%s') WHERE path = '%s';",
          renameEvent.getSrcPath(), renameEvent.getDstPath(), renameEvent.getSrcPath()));
      if (info.isdir()) {
        ret.add(String.format("UPDATE files SET path = replace(path, '%s', '%s') WHERE path LIKE '%s/%%';",
            renameEvent.getSrcPath(), renameEvent.getDstPath(), renameEvent.getSrcPath()));
      }
    }
    return ret;
  }

  private String getMetaDataUpdateSql(Event.MetadataUpdateEvent metadataUpdateEvent) {
    switch (metadataUpdateEvent.getMetadataType()) {
      case TIMES:
        if (metadataUpdateEvent.getMtime() > 0 && metadataUpdateEvent.getAtime() > 0) {
          return String.format(
            "UPDATE files SET modification_time = %s, access_time = %s WHERE path = '%s';",
            metadataUpdateEvent.getMtime(),
            metadataUpdateEvent.getAtime(),
            metadataUpdateEvent.getPath());
        } else if (metadataUpdateEvent.getMtime() > 0) {
          return String.format(
            "UPDATE files SET modification_time = %s WHERE path = '%s';",
            metadataUpdateEvent.getMtime(),
            metadataUpdateEvent.getPath());
        } else if (metadataUpdateEvent.getAtime() > 0) {
          return String.format(
            "UPDATE files SET access_time = %s WHERE path = '%s';",
            metadataUpdateEvent.getAtime(),
            metadataUpdateEvent.getPath());
        } else {
          return "";
        }
      case OWNER:
        //Todo
        break;
      case PERMS:
        return String.format(
            "UPDATE files SET permission = %s WHERE path = '%s';",
            metadataUpdateEvent.getPerms().toShort(), metadataUpdateEvent.getPath());
      case REPLICATION:
        return String.format(
            "UPDATE files SET block_replication = %s WHERE path = '%s';",
            metadataUpdateEvent.getReplication(), metadataUpdateEvent.getPath());
      case XATTRS:
        //Todo
        if (LOG.isDebugEnabled()) {
          String message = "\n";
          for (XAttr xAttr : metadataUpdateEvent.getxAttrs()) {
            message += xAttr.toString() + "\n";
          }
          LOG.debug(message);
        }
        break;
      case ACLS:
        return "";
    }
    return "";
  }

  private List<String> getAppendSql(Event.AppendEvent appendEvent) {
    //Do nothing;
    return Arrays.asList();
  }

  private List<String> getUnlinkSql(Event.UnlinkEvent unlinkEvent) {
    return Arrays.asList(String.format("DELETE FROM files WHERE path LIKE '%s%%';", unlinkEvent.getPath()));
  }
}
