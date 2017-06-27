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
package org.smartdata.actions.hdfs;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.actions.ActionException;

import java.util.List;
import java.util.Map;

/**
 * Check and return file blocks storage location.
 */
public class CheckStorageAction extends HdfsAction {
  private static final Logger LOG = LoggerFactory.getLogger(WriteFileAction.class);

  private String fileName;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    fileName = args.get(FILE_PATH);
  }

  @Override
  protected void execute() throws Exception {
    try {
      HdfsFileStatus fileStatus = dfsClient.getFileInfo(fileName);
      if (fileStatus == null) {
        throw new ActionException("File does not exist.");
      }
      long length = fileStatus.getLen();
      List<LocatedBlock> locatedBlocks = dfsClient.getLocatedBlocks(
          fileName, 0, length).getLocatedBlocks();
      for (LocatedBlock locatedBlock : locatedBlocks) {
        StringBuilder blockInfo = new StringBuilder();
        blockInfo.append("File offset = ")
            .append(locatedBlock.getStartOffset())
            .append(", ");
        blockInfo.append("Block locations = {");
        for (DatanodeInfo datanodeInfo : locatedBlock.getLocations()) {
          blockInfo.append(datanodeInfo.getName());
          if (datanodeInfo instanceof DatanodeInfoWithStorage) {
            blockInfo.append("[")
                .append(((DatanodeInfoWithStorage)datanodeInfo).getStorageType())
                .append("]");
          }
          blockInfo.append(" ");
        }
        blockInfo.append("}");
        this.appendResult(blockInfo.toString());
      }
    } catch (Exception e) {
      LOG.error("CheckStorageAction failed", e);
      throw new ActionException(e);
    }
  }
}
