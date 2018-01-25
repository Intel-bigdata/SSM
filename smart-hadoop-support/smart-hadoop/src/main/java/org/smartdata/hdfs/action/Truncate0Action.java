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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.annotation.ActionSignature;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.Map;

/**
 * action to set file length to zero
 */
@ActionSignature(
    actionId = "truncate0",
    displayName = "truncate0",
    usage = HdfsAction.FILE_PATH + " $src "
)
public class Truncate0Action extends HdfsAction {
  private static final Logger LOG = LoggerFactory.getLogger(TruncateAction.class);
  private String srcPath;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    srcPath = args.get(FILE_PATH);
  }

  @Override
  protected void execute() throws Exception {
    if (srcPath == null) {
      throw new IllegalArgumentException("File src is missing.");
    }
    setLen2Zero(srcPath);
  }

  private boolean setLen2Zero(String srcPath) throws IOException {
    if (srcPath.startsWith("hdfs")) {
      // TODO read conf from files
      Configuration conf = new Configuration();
      DistributedFileSystem fs = new DistributedFileSystem();
      fs.initialize(URI.create(srcPath), conf);

      return setLen2Zero(fs, srcPath);
    } else {
      return setLen2Zero(dfsClient, srcPath);
    }
  }

  private boolean setLen2Zero(DFSClient client, String src) throws IOException {
    // return client.truncate(src, 0);
    // Delete file and create file
    // Save the metadata
    HdfsFileStatus fileStatus = client.getFileInfo(src);
    // AclStatus aclStatus = client.getAclStatus(src);
    Map<String, byte[]> XAttr = client.getXAttrs(src);
    // Delete file
    client.delete(src, true);
    // Create file
    client.create(src, true);
    // Set metadata
    client.setOwner(src, fileStatus.getOwner(), fileStatus.getGroup());
    client.setPermission(src, fileStatus.getPermission());
    client.setReplication(src, fileStatus.getReplication());
    client.setStoragePolicy(src, "Cold");
    client.setTimes(src, fileStatus.getAccessTime(),
            client.getFileInfo(src).getModificationTime());
    // client.setAcl(src, aclStatus.getEntries());
    for(Map.Entry<String, byte[]> entry : XAttr.entrySet()){
      client.setXAttr(src, entry.getKey(), entry.getValue(),
              EnumSet.of(XAttrSetFlag.CREATE, XAttrSetFlag.REPLACE));
    }
    return true;
  }

  private boolean setLen2Zero(DistributedFileSystem fileSystem, String src) throws IOException {
    // return fileSystem.truncate(new Path(src), 0);
    // Delete file and create file
    // Save the metadata
    FileStatus fileStatus = fileSystem.getFileStatus(new Path(src));
    // AclStatus aclStatus = fileSystem.getAclStatus(new Path(src));
    Map<String, byte[]> XAttr = fileSystem.getXAttrs(new Path(src));
    // Delete file
    fileSystem.delete(new Path(src), true);
    // Create file
    fileSystem.create(new Path(src), true);
    // Set metadata
    fileSystem.setOwner(new Path(src), fileStatus.getOwner(), fileStatus.getGroup());
    fileSystem.setPermission(new Path(src), fileStatus.getPermission());
    fileSystem.setReplication(new Path(src), fileStatus.getReplication());
    fileSystem.setStoragePolicy(new Path(src), "Cold");
    fileSystem.setTimes(new Path(src), fileStatus.getAccessTime(),
            fileSystem.getFileStatus(new Path(src)).getModificationTime());
    // fileSystem.setAcl(new Path(src), aclStatus.getEntries());
    for(Map.Entry<String, byte[]> entry : XAttr.entrySet()){
      fileSystem.setXAttr(new Path(src), entry.getKey(), entry.getValue(),
              EnumSet.of(XAttrSetFlag.CREATE, XAttrSetFlag.REPLACE));
    }
    return true;
  }
}
