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
package org.smartdata.model;

public class DetailedFileAction extends ActionInfo {
  private String filePath;
  private long fileLength;
  private String src;
  private String target;


  public DetailedFileAction(ActionInfo actionInfo) {
    super(actionInfo.getActionId(), actionInfo.getCmdletId(),
        actionInfo.getActionName(), actionInfo.getArgs(),
        actionInfo.getResult(), actionInfo.getLog(),
        actionInfo.isSuccessful(), actionInfo.getCreateTime(),
        actionInfo.isFinished(), actionInfo.getFinishTime(),
        actionInfo.getProgress());
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public long getFileLength() {
    return fileLength;
  }

  public void setFileLength(long fileLength) {
    this.fileLength = fileLength;
  }

  public String getSrc() {
    return src;
  }

  public void setSrc(String src) {
    this.src = src;
  }

  public String getTarget() {
    return target;
  }

  public void setTarget(String target) {
    this.target = target;
  }
}
