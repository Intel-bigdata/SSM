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

import com.alibaba.druid.sql.dialect.oracle.ast.clause.OracleWithSubqueryEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.Utils;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.model.FileInfo;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;

/**
 * action to set MetaData of file
 */
@ActionSignature(
    actionId = "metadata",
    displayName = "metadata",
    usage = HdfsAction.FILE_PATH + " $src "
)
public class MetaFileAction extends HdfsAction {
  private static final Logger LOG = LoggerFactory.getLogger(MetaFileAction.class);
  public static final String OWNER_NAME = "-owername";
  public static final String GROUP_NAME = "-groupname";
  public static final String BLOCK_REPLICATION = "-replication";
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

    if (args.containsKey(BLOCK_REPLICATION)){
      this.replication = Short.parseShort(args.get(BLOCK_REPLICATION));
    }

    if (args.containsKey(PERMISSION)) {
      this.permission = Short.parseShort(args.get(PERMISSION));
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

    FileInfo fileInfo = new FileInfo(srcPath, 0, 0, false, replication, 0, mTime, aTime, permission, ownerName, groupName, (byte) 1);

    changeFileMetaData(srcPath, fileInfo);
  }

  private boolean changeFileMetaData(String srcFile, FileInfo fileInfo) throws IOException {
    if (srcFile.startsWith("hdfs")) {
      //change file metadata in remote cluster
      // TODO read conf from files
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(URI.create(srcFile), conf);

      if (fileInfo.getOwner() != null) {
        fs.setOwner(new Path(srcFile), fileInfo.getOwner(), fs.getFileStatus(new Path(srcFile)).getGroup());
      }

      if (fileInfo.getGroup() != null) {
        fs.setOwner(new Path(srcFile), fs.getFileStatus(new Path(srcFile)).getOwner(), fileInfo.getGroup());
      }

      if (fileInfo.getBlock_replication() != -1) {
        fs.setReplication(new Path(srcFile), fileInfo.getBlock_replication());
      }

      if (fileInfo.getPermission() != -1) {
        fs.setPermission(new Path(srcFile), new FsPermission(fileInfo.getPermission()));
      }

      if (fileInfo.getAccess_time() != -1) {
        fs.setTimes(new Path(srcFile), fs.getFileStatus(new Path(srcFile)).getModificationTime()
            , fileInfo.getAccess_time());
      }

      if (fileInfo.getModification_time() != -1) {
        fs.setTimes(new Path(srcFile), fileInfo.getModification_time()
            , fs.getFileStatus(new Path(srcFile)).getAccessTime());
      }


      

      return true;
    } else {


      return true;
    }

  }
}
