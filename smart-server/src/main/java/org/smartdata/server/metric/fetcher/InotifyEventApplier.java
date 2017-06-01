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
package org.smartdata.server.metric.fetcher;

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.smartdata.server.metastore.sql.DBAdapter;
import org.smartdata.server.metastore.sql.DBAdapter;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This is a very preliminary and buggy applier, can further enhance by referring to
 * {@link org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader}
 */
public class InotifyEventApplier {
  private final DBAdapter adapter;
  private DFSClient client;

  public InotifyEventApplier(DBAdapter adapter, DFSClient client) {
    this.adapter = adapter;
    this.client = client;
  }

  public void apply(List<Event> events) throws IOException, SQLException {
    List<String> statements = new ArrayList<>();
    for (Event event : events) {
      String statement = getSqlStatement(event);
      if (statement != null && !statement.isEmpty()){
        statements.add(statement);
      }
    }
    this.adapter.execute(statements);
  }

  public void apply(Event[] events) throws IOException, SQLException {
    this.apply(Arrays.asList(events));
  }

  private String getSqlStatement(Event event) throws IOException {
    switch (event.getEventType()) {
      case CREATE:
        return this.getCreateSql((Event.CreateEvent)event);
      case CLOSE:
        return this.getCloseSql((Event.CloseEvent)event);
//      case TRUNCATE:
//        return this.getTruncateSql((Event.TruncateEvent)event);
      case RENAME:
        return this.getRenameSql((Event.RenameEvent)event);
      case METADATA:
        return this.getMetaDataUpdateSql((Event.MetadataUpdateEvent)event);
      case APPEND:
        return this.getAppendSql((Event.AppendEvent)event);
      case UNLINK:
        return this.getUnlinkSql((Event.UnlinkEvent)event);
    }
    return "";
  }

  //Todo: times and ec policy id, etc.
  private String getCreateSql(Event.CreateEvent createEvent) throws IOException {
    HdfsFileStatus fileStatus = client.getFileInfo(createEvent.getPath());
    boolean isDir = createEvent.getiNodeType() == Event.CreateEvent.INodeType.DIRECTORY;
    return String.format(
        "INSERT INTO `files` (path, fid, block_replication, "
            + "block_size, is_dir, permission) VALUES ('%s', %s, %s, %s, %s, %s);",
        createEvent.getPath(),
        fileStatus.getFileId(),
        createEvent.getReplication(),
        createEvent.getDefaultBlockSize(),
        isDir ? 1 : 0,
        createEvent.getPerms().toShort());
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

  private String getRenameSql(Event.RenameEvent renameEvent) {
    return String.format(
        "UPDATE files SET path = replace(path, '%s', '%s');",
        renameEvent.getSrcPath(), renameEvent.getDstPath());
  }

  private String getMetaDataUpdateSql(Event.MetadataUpdateEvent metadataUpdateEvent) {
    switch (metadataUpdateEvent.getMetadataType()) {
      case TIMES:
        return String.format(
            "UPDATE files SET modification_time = %s, access_time = %s WHERE path = '%s';",
            metadataUpdateEvent.getMtime(),
            metadataUpdateEvent.getAtime(),
            metadataUpdateEvent.getPath());
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
        break;
      case ACLS:
        return "";
    }
    return "";
  }

  private String getAppendSql(Event.AppendEvent appendEvent) {
    //Do nothing;
    return "";
  }

  private String getUnlinkSql(Event.UnlinkEvent unlinkEvent) {
    return String.format("DELETE FROM files WHERE path LIKE '%s%%';", unlinkEvent.getPath());
  }
}
