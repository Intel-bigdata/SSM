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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyState;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.smartdata.action.ActionException;
import org.smartdata.conf.SmartConf;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

abstract public class ErasureCodingBase extends HdfsAction {
  public static final String BUF_SIZE = "-bufSize";
  protected String srcPath;
  protected String tmpPath;
  protected int bufferSize = 1024 * 1024;
  protected float progress;
  public static final String TMP_PATH = "-tmp";

  protected void convert(SmartConf conf, String ecPolicyName) throws ActionException {
    HdfsDataOutputStream outputStream = null;
    DFSInputStream in = null;
    DFSOutputStream out = null;
    try {
      // append the file to acquire the lock to avoid modifying, no real appending occurs.
      outputStream =
          dfsClient.append(srcPath, bufferSize, EnumSet.of(CreateFlag.APPEND), null, null);
      long blockSize = conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
      in = dfsClient.open(srcPath, bufferSize, true);
      HdfsFileStatus fileStatus = dfsClient.getFileInfo(srcPath);
      // use the same FsPermission as srcPath
      FsPermission permission = fileStatus.getPermission();
      out = dfsClient.create(tmpPath, permission, EnumSet.of(CreateFlag.CREATE), true,
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
        if (outputStream != null) {
          outputStream.close();
        }
      } catch (IOException ex) {
        throw new ActionException(ex);
      }
    }
  }

  public void ValidateEcPolicy(String ecPolicyName) throws Exception {
    Map<String, ErasureCodingPolicyState> ecPolicyNameToState = new HashMap<>();
    for (ErasureCodingPolicyInfo info : dfsClient.getErasureCodingPolicies()) {
      ecPolicyNameToState.put(info.getPolicy().getName(), info.getState());
    }
    if (!ecPolicyNameToState.keySet().contains(ecPolicyName)) {
      throw new ActionException("The EC policy " + ecPolicyName + " is not supported!");
    } else if (ecPolicyNameToState.get(ecPolicyName) == ErasureCodingPolicyState.DISABLED
        || ecPolicyNameToState.get(ecPolicyName) == ErasureCodingPolicyState.REMOVED) {
      throw new ActionException("The EC policy " + ecPolicyName + " is not enabled!");
    }
  }
}