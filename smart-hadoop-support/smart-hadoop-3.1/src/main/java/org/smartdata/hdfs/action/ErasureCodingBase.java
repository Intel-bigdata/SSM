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

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.ActionException;
import org.smartdata.conf.SmartConf;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

abstract public class ErasureCodingBase extends HdfsAction {
  private static final Logger LOG =
      LoggerFactory.getLogger(ErasureCodingBase.class);
  public static final String BUF_SIZE = "-bufSize";
  protected String srcPath;
  protected String ecTmpPath;
  protected int bufferSize = 1024 * 1024;
  protected float progress;
  // The value for -ecTmp is assigned by ErasureCodingScheduler.
  public static final String EC_TMP = "-ecTmp";
  public static final String REPLICATION_POLICY_NAME =
      SystemErasureCodingPolicies.getReplicationPolicy().getName();

  protected void convert(SmartConf conf, String ecPolicyName) throws ActionException {
    DFSInputStream in = null;
    DFSOutputStream out = null;
    try {
      long blockSize = conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
          DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
      in = dfsClient.open(srcPath, bufferSize, true);
      HdfsFileStatus fileStatus = dfsClient.getFileInfo(srcPath);
      // use the same FsPermission as srcPath
      FsPermission permission = fileStatus.getPermission();
      out = dfsClient.create(ecTmpPath, permission, EnumSet.of(CreateFlag.CREATE), true,
          (short) 1, blockSize, null, bufferSize, null, null, ecPolicyName);
      long bytesRemaining = fileStatus.getLen();
      byte[] buf = new byte[bufferSize];
      while (bytesRemaining > 0L) {
        int bytesToRead =
            (int) (bytesRemaining < (long) buf.length ? bytesRemaining :
                (long) buf.length);
        int bytesRead = in.read(buf, 0, bytesToRead);
        if (bytesRead == -1) {
          break;
        }
        out.write(buf, 0, bytesRead);
        bytesRemaining -= (long) bytesRead;
        this.progress = (float) (fileStatus.getLen() - bytesRemaining) / fileStatus.getLen();
      }
    } catch (Exception ex) {
      throw new ActionException(ex);
    } finally {
      try {
        if (in != null) {
          in.close();
        }
        if (out != null) {
          out.close();
        }
      } catch (IOException ex) {
        throw new ActionException(ex);
      }
    }
  }

  // set attributes for dest to keep them consistent with their counterpart of src
  protected void setAttributes(String src, HdfsFileStatus fileStatus, String dest)
      throws IOException {
    dfsClient.setOwner(dest, fileStatus.getOwner(), fileStatus.getGroup());
    dfsClient.setPermission(dest, fileStatus.getPermission());
    dfsClient.setStoragePolicy(dest, dfsClient.getStoragePolicy(src).getName());
    // check whether mtime is changed after rename
    dfsClient.setTimes(dest, fileStatus.getModificationTime(), fileStatus.getAccessTime());
    boolean aclsEnabled = getContext().getConf().getBoolean(
        DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY,
        DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_DEFAULT);
    if (aclsEnabled) {
      dfsClient.setAcl(dest, dfsClient.getAclStatus(src).getEntries());
    }
    for(Map.Entry<String, byte[]> entry : dfsClient.getXAttrs(src).entrySet()) {
      dfsClient.setXAttr(dest, entry.getKey(), entry.getValue(),
          EnumSet.of(XAttrSetFlag.CREATE, XAttrSetFlag.REPLACE));
    }
  }
}