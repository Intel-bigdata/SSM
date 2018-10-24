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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.AbstractActionFactory;
import org.smartdata.action.SmartAction;
import org.smartdata.hdfs.scheduler.ErasureCodingScheduler;

import java.util.Arrays;
import java.util.List;

/**
 * Built-in smart actions for HDFS system.
 */
public class HdfsActionFactory extends AbstractActionFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsActionFactory.class);
  public static final List<String> HDFS3_ACTION_CLASSES = Arrays.asList(
      "org.smartdata.hdfs.action.ListErasureCodingPolicy",
      "org.smartdata.hdfs.action.CheckErasureCodingPolicy",
      "org.smartdata.hdfs.action.ErasureCodingAction",
      "org.smartdata.hdfs.action.UnErasureCodingAction",
      "org.smartdata.hdfs.action.AddErasureCodingPolicy",
      "org.smartdata.hdfs.action.RemoveErasureCodingPolicy",
      "org.smartdata.hdfs.action.EnableErasureCodingPolicy",
      "org.smartdata.hdfs.action.DisableErasureCodingPolicy");

  static {
    addAction(AllSsdFileAction.class);
    addAction(AllDiskFileAction.class);
    addAction(OneSsdFileAction.class);
    addAction(OneDiskFileAction.class);
    addAction(RamDiskFileAction.class);
    addAction(ArchiveFileAction.class);
    addAction(CacheFileAction.class);
    addAction(UncacheFileAction.class);
    addAction(ReadFileAction.class);
    addAction(WriteFileAction.class);
    addAction(CheckStorageAction.class);
    addAction(SetXAttrAction.class);
//    addAction("blockec", BlockErasureCodeFileAction.class);
    addAction(CopyFileAction.class);
    addAction(DeleteFileAction.class);
    addAction(RenameFileAction.class);
    addAction(ListFileAction.class);
    addAction(ConcatFileAction.class);
    addAction(AppendFileAction.class);
    addAction(MergeFileAction.class);
    addAction(MetaDataAction.class);
    addAction(Copy2S3Action.class);
    addAction(TruncateAction.class);
    addAction(Truncate0Action.class);
    addAction(SmallFileCompactAction.class);
    addAction(SmallFileUncompactAction.class);
    addAction(CheckSumAction.class);
//    addAction("list", ListFileAction.class);
//    addAction("fsck", FsckAction.class);
//    addAction("diskbalance", DiskBalanceAction.class);
//    addAction("clusterbalance", ClusterBalanceAction.class);
//    addAction("setstoragepolicy", SetStoragePolicyAction.class);
    if (ErasureCodingScheduler.isECSupported()) {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      if (loader == null) {
        loader = ClassLoader.getSystemClassLoader();
      }
      try {
        for (String classString : HDFS3_ACTION_CLASSES) {
          addAction((Class<SmartAction>) loader.loadClass(classString));
        }
      } catch (ClassNotFoundException ex) {
        LOG.error("Class not found!", ex);
      }
    }
  }
}
