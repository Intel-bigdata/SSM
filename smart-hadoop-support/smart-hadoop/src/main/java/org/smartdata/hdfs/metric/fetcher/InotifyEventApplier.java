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
import org.smartdata.conf.SmartConf;
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

  //check if the dir is in ignoreList

  public void apply(Event[] events) throws IOException, MetaStoreException {
    this.apply(Arrays.asList(events));
  }

  private List<String> getSqlStatement(Event event) throws IOException, MetaStoreException {
    LOG.debug("Even Type = {}", event.getEventType().toString());
    switch (event.getEventType()) {
      case CREATE:
        LOG.trace("event type:" + event.getEventType().name() +
            ", path:" + ((Event.CreateEvent) event).getPath());
        return Arrays.asList(this.getCreateSql((Event.CreateEvent) event));
      case CLOSE:
        LOG.trace("event type:" + event.getEventType().name() +
            ", path:" + ((Event.CloseEvent) event).getPath());
        return Arrays.asList(this.getCloseSql((Event.CloseEvent) event));
      case RENAME:
        LOG.trace("event type:" + event.getEventType().name() +
            ", src path:" + ((Event.RenameEvent) event).getSrcPath() +
            ", dest path:" + ((Event.RenameEvent) event).getDstPath());
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
        return this.getUnlinkSql((Event.UnlinkEvent)event);
    }
    return Arrays.asList();
  }

  //Todo: times and ec policy id, etc.
  private String getCreateSql(Event.CreateEvent createEvent) throws IOException, MetaStoreException {
    HdfsFileStatus fileStatus = client.getFileInfo(createEvent.getPath());
    if (fileStatus == null) {
      LOG.debug("Can not get HdfsFileStatus for file " + createEvent.getPath());
      return "";
    }
    FileInfo fileInfo = HadoopUtil.convertFileStatus(fileStatus, createEvent.getPath());

    if (inBackup(fileInfo.getPath())) {
      if (!fileInfo.isdir()) {

        // ignore dir
        FileDiff fileDiff = new FileDiff(FileDiffType.APPEND);
        fileDiff.setSrc(fileInfo.getPath());
        fileDiff.getParameters().put("-offset", String.valueOf(0));
        // Note that "-length 0" means create an empty file
        fileDiff.getParameters()
            .put("-length", String.valueOf(fileInfo.getLength()));
        // TODO add support in CopyFileAction or split into two file diffs
        //add modification_time and access_time to filediff
        fileDiff.getParameters().put("-mtime", "" + fileInfo.getModificationTime());
        // fileDiff.getParameters().put("-atime", "" + fileInfo.getAccessTime());
        //add owner to filediff
        fileDiff.getParameters().put("-owner", "" + fileInfo.getOwner());
        fileDiff.getParameters().put("-group", "" + fileInfo.getGroup());
        //add Permission to filediff
        fileDiff.getParameters().put("-permission", "" + fileInfo.getPermission());
        //add replication count to file diff
        fileDiff.getParameters().put("-replication", "" + fileInfo.getBlockReplication());
        metaStore.insertFileDiff(fileDiff);
      }
    }
    metaStore.insertFile(fileInfo);
    return "";
  }

  private boolean inBackup(String src) throws MetaStoreException {
    if (metaStore.srcInbackup(src)) {
      return true;
    }
    return false;
  }

  //Todo: should update mtime? atime?
  private String getCloseSql(Event.CloseEvent closeEvent) throws IOException, MetaStoreException {
    FileDiff fileDiff = new FileDiff(FileDiffType.APPEND);
    fileDiff.setSrc(closeEvent.getPath());
    long newLen = closeEvent.getFileSize();
    long currLen = 0l;
    // TODO make sure offset is correct
    if (inBackup(closeEvent.getPath())) {
      FileInfo fileInfo = metaStore.getFile(closeEvent.getPath());
      if (fileInfo == null) {
        // TODO add metadata
        currLen = 0;
      } else {
        currLen = fileInfo.getLength();
      }
      if (currLen != newLen) {
        fileDiff.getParameters().put("-offset", String.valueOf(currLen));
        fileDiff.getParameters()
            .put("-length", String.valueOf(newLen - currLen));
        metaStore.insertFileDiff(fileDiff);
      }
    }
    return String.format(
        "UPDATE file SET length = %s, modification_time = %s WHERE path = '%s';",
        closeEvent.getFileSize(), closeEvent.getTimestamp(), closeEvent.getPath());
  }

  //Todo: should update mtime? atime?
//  private String getTruncateSql(Event.TruncateEvent truncateEvent) {
//    return String.format(
//        "UPDATE file SET length = %s, modification_time = %s WHERE path = '%s';",
//        truncateEvent.getFileSize(), truncateEvent.getTimestamp(), truncateEvent.getPath());
//  }

  private List<String> getRenameSql(Event.RenameEvent renameEvent)
      throws IOException, MetaStoreException {
    List<String> ret = new ArrayList<>();
    HdfsFileStatus status = client.getFileInfo(renameEvent.getDstPath());
    FileDiff fileDiff = new FileDiff(FileDiffType.RENAME);
    if (inBackup(renameEvent.getSrcPath())) {
      fileDiff.setSrc(renameEvent.getSrcPath());
      fileDiff.getParameters().put("-dest",
          renameEvent.getDstPath());
      metaStore.insertFileDiff(fileDiff);
    }
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
      ret.add(String.format("UPDATE file SET path = replace(path, '%s', '%s') WHERE path = '%s';",
          renameEvent.getSrcPath(), renameEvent.getDstPath(), renameEvent.getSrcPath()));
      if (info.isdir()) {
        ret.add(String.format("UPDATE file SET path = replace(path, '%s', '%s') WHERE path LIKE '%s/%%';",
            renameEvent.getSrcPath(), renameEvent.getDstPath(), renameEvent.getSrcPath()));
      }
    }
    return ret;
  }

  private String getMetaDataUpdateSql(Event.MetadataUpdateEvent metadataUpdateEvent) throws MetaStoreException {

    FileDiff fileDiff = null;
    if (inBackup(metadataUpdateEvent.getPath())) {
      fileDiff = new FileDiff(FileDiffType.METADATA);
      fileDiff.setSrc(metadataUpdateEvent.getPath());
    }
    switch (metadataUpdateEvent.getMetadataType()) {
      case TIMES:
        if (metadataUpdateEvent.getMtime() > 0 && metadataUpdateEvent.getAtime() > 0) {
          if (fileDiff != null) {
            fileDiff.getParameters().put("-mtime", "" + metadataUpdateEvent.getMtime());
            // fileDiff.getParameters().put("-access_time", "" + metadataUpdateEvent.getAtime());
            metaStore.insertFileDiff(fileDiff);
          }
          return String.format(
            "UPDATE file SET modification_time = %s, access_time = %s WHERE path = '%s';",
            metadataUpdateEvent.getMtime(),
            metadataUpdateEvent.getAtime(),
            metadataUpdateEvent.getPath());
        } else if (metadataUpdateEvent.getMtime() > 0) {
          if (fileDiff != null) {
            fileDiff.getParameters().put("-mtime", "" + metadataUpdateEvent.getMtime());
            metaStore.insertFileDiff(fileDiff);
          }
          return String.format(
            "UPDATE file SET modification_time = %s WHERE path = '%s';",
            metadataUpdateEvent.getMtime(),
            metadataUpdateEvent.getPath());
        } else if (metadataUpdateEvent.getAtime() > 0) {
          // if (fileDiff != null) {
          //   fileDiff.getParameters().put("-access_time", "" + metadataUpdateEvent.getAtime());
          //   metaStore.insertFileDiff(fileDiff);
          // }
          return String.format(
            "UPDATE file SET access_time = %s WHERE path = '%s';",
            metadataUpdateEvent.getAtime(),
            metadataUpdateEvent.getPath());
        } else {
          return "";
        }
      case OWNER:
        if (fileDiff != null) {
          fileDiff.getParameters().put("-owner", "" + metadataUpdateEvent.getOwnerName());
          metaStore.insertFileDiff(fileDiff);
        }
        //Todo
        break;
      case PERMS:
        if (fileDiff != null) {
          fileDiff.getParameters().put("-permission", "" + metadataUpdateEvent.getPerms().toShort());
          metaStore.insertFileDiff(fileDiff);
        }
        return String.format(
            "UPDATE file SET permission = %s WHERE path = '%s';",
            metadataUpdateEvent.getPerms().toShort(), metadataUpdateEvent.getPath());
      case REPLICATION:
        if (fileDiff != null) {
          fileDiff.getParameters().put("-replication", "" + metadataUpdateEvent.getReplication());
          metaStore.insertFileDiff(fileDiff);
        }
        return String.format(
            "UPDATE file SET block_replication = %s WHERE path = '%s';",
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

  private List<String> getUnlinkSql(Event.UnlinkEvent unlinkEvent) throws MetaStoreException {
    List<FileInfo> fileInfos = metaStore.getFilesByPrefix(unlinkEvent.getPath());
    for (FileInfo fileInfo:fileInfos) {
      if (fileInfo.isdir()) {
        continue;
      }
      if (inBackup(unlinkEvent.getPath())) {
        FileDiff fileDiff = new FileDiff(FileDiffType.DELETE);
        fileDiff.setSrc(unlinkEvent.getPath());
        metaStore.insertFileDiff(fileDiff);
      }
    }
    return Arrays.asList(String.format("DELETE FROM file WHERE path LIKE '%s%%';", unlinkEvent.getPath()));
  }
}
