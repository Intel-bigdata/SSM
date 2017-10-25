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
package org.smartdata.hdfs.action;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.model.FileInfo;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * action to set MetaData of file
 */
@ActionSignature(
    actionId = "metadata",
    displayName = "metadata",
    usage = HdfsAction.FILE_PATH + " $src " + MetaDataAction.OWNER_NAME + " $owner " +
        MetaDataAction.GROUP_NAME + " $group " + MetaDataAction.BLOCK_REPLICATION + " $replication " +
        MetaDataAction.PERMISSION + " $permission " + MetaDataAction.MTIME + " $mtime " +
        MetaDataAction.ATIME + " $atime"
)
public class MetaDataAction extends HdfsAction {
  private static final Logger LOG = LoggerFactory.getLogger(MetaDataAction.class);
  public static final String OWNER_NAME = "-owner";
  public static final String GROUP_NAME = "-group";
  public static final String BLOCK_REPLICATION = "-replication";
  // only support input like 777
  public static final String PERMISSION = "-permission";
  public static final String MTIME = "-mtime";
  public static final String ATIME = "-atime";

  private String srcPath;
  private String ownerName;
  private String groupName;
  private short replication;
  private short permission;
  private long aTime;
  private long mTime;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    srcPath = args.get(FILE_PATH);

    ownerName = null;
    groupName = null;
    replication = -1;
    permission = -1;
    aTime = -1;
    mTime = -1;

    if (args.containsKey(OWNER_NAME)) {
      this.ownerName = args.get(OWNER_NAME);
    }
    if (args.containsKey(GROUP_NAME)) {
      this.groupName = args.get(GROUP_NAME);
    }
    if (args.containsKey(BLOCK_REPLICATION)) {
      this.replication = Short.parseShort(args.get(BLOCK_REPLICATION));
    }
    if (args.containsKey(PERMISSION)) {
      FsPermission fsPermission = new FsPermission(args.get(PERMISSION));
      this.permission = fsPermission.toShort();
    }
    if (args.containsKey(MTIME)) {
      this.mTime = Long.parseLong(args.get(MTIME));
    }
    if (args.containsKey(ATIME)) {
      this.aTime = Long.parseLong(args.get(ATIME));
    }
  }

  @Override
  protected void execute() throws Exception {
    if (srcPath == null) {
      throw new IllegalArgumentException("File src is missing.");
    }

    FileInfo fileInfo =
        new FileInfo(
            srcPath,
            0,
            0,
            false,
            replication,
            0,
            mTime,
            aTime,
            permission,
            ownerName,
            groupName,
            (byte) 1);

    changeFileMetaData(srcPath, fileInfo);
  }

  private boolean changeFileMetaData(String srcFile, FileInfo fileInfo) throws IOException {
    try {
      if (srcFile.startsWith("hdfs")) {
        // change file metadata in remote cluster
        // TODO read conf from files
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(srcFile), conf);

        if (fileInfo.getOwner() != null) {
          fs.setOwner(
              new Path(srcFile),
              fileInfo.getOwner(),
              fs.getFileStatus(new Path(srcFile)).getGroup());
        }
        if (fileInfo.getGroup() != null) {
          fs.setOwner(
              new Path(srcFile),
              fs.getFileStatus(new Path(srcFile)).getOwner(),
              fileInfo.getGroup());
        }
        if (fileInfo.getBlockReplication() != -1) {
          fs.setReplication(new Path(srcFile), fileInfo.getBlockReplication());
        }
        if (fileInfo.getPermission() != -1) {
          fs.setPermission(new Path(srcFile), new FsPermission(fileInfo.getPermission()));
        }
        if (fileInfo.getAccessTime() != -1) {
          fs.setTimes(
              new Path(srcFile),
              fs.getFileStatus(new Path(srcFile)).getModificationTime(),
              fileInfo.getAccessTime());
        }
        if (fileInfo.getModificationTime() != -1) {
          fs.setTimes(
              new Path(srcFile),
              fileInfo.getModificationTime(),
              fs.getFileStatus(new Path(srcFile)).getAccessTime());
        }
        return true;
      } else {
        // change file metadata in local cluster
        if (fileInfo.getOwner() != null) {
          dfsClient.setOwner(
              srcFile, fileInfo.getOwner(), dfsClient.getFileInfo(srcFile).getGroup());
        }
        if (fileInfo.getGroup() != null) {
          dfsClient.setOwner(
              srcFile, dfsClient.getFileInfo(srcFile).getOwner(), fileInfo.getGroup());
        }
        if (fileInfo.getBlockReplication() != -1) {
          dfsClient.setReplication(srcFile, fileInfo.getBlockReplication());
        }
        if (fileInfo.getPermission() != -1) {
          dfsClient.setPermission(srcFile, new FsPermission(fileInfo.getPermission()));
        }
        if (fileInfo.getAccessTime() != -1) {
          dfsClient.setTimes(
              srcFile,
              dfsClient.getFileInfo(srcFile).getModificationTime(),
              fileInfo.getAccessTime());
        }
        if (fileInfo.getModificationTime() != -1) {
          dfsClient.setTimes(
              srcFile,
              fileInfo.getModificationTime(),
              dfsClient.getFileInfo(srcFile).getAccessTime());
        }
        return true;
      }
    } catch (Exception e) {
      LOG.debug("Metadata cannot be applied", e);
    }
    return false;
  }
}
