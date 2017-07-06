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

import org.smartdata.actions.AbstractActionFactory;

/**
 * Built-in smart actions for HDFS system.
 */
public class HdfsActionFactory extends AbstractActionFactory {

  static {
    addAction("allssd", AllSsdFileAction.class);
    addAction("onessd", OneSsdFileAction.class);
    addAction("archive", ArchiveFileAction.class);
    addAction("cache", CacheFileAction.class);
    addAction("uncache", UncacheFileAction.class);
    addAction("read", ReadFileAction.class);
    addAction("write", WriteFileAction.class);
    addAction("checkstorage", CheckStorageAction.class);
//    addAction("stripec", StripErasureCodeFileAction.class);
//    addAction("blockec", BlockErasureCodeFileAction.class);
//    addAction("copy", CopyFileAction.class);
//    addAction("list", ListFileAction.class);
//    addAction("fsck", FsckAction.class);
//    addAction("diskbalance", DiskBalanceAction.class);
//    addAction("clusterbalance", ClusterBalanceAction.class);
//    addAction("setstoragepolicy", SetStoragePolicyAction.class);
  }
}
