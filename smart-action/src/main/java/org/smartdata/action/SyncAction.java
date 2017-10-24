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
package org.smartdata.action;

import org.smartdata.action.annotation.ActionSignature;

/**
 * Sync action is an abstract action for backup and copy.
 * Users can submit a sync action with detailed src path and
 * dest path, e.g., "sync -src /test/1 -dest hdfs:/remoteIP:port/test/1"
 */
@ActionSignature(
    actionId = "sync",
    displayName = "sync",
    usage = SyncAction.SRC + " $src" + SyncAction.DEST + " $dest"
)
public class SyncAction extends SmartAction {
  // related to fileDiff.src
  public static final String SRC = "-src";
  // related to remote cluster and fileDiff.src
  public static final String DEST = "-dest";

  @Override
  protected void execute() throws Exception {
  }
}
