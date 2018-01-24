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

import org.apache.hadoop.fs.XAttrSetFlag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.ActionException;
import org.smartdata.action.annotation.ActionSignature;

import java.util.EnumSet;
import java.util.Map;

/**
 * An action to setXAttr to a given file.
 */
@ActionSignature(
    actionId = "setxattr",
    displayName = "setxattr",
    usage = HdfsAction.FILE_PATH + " $src " + SetXAttrAction.ATT_NAME +
        " $name " + SetXAttrAction.ATT_VALUE + " $value"
)
public class SetXAttrAction extends HdfsAction {
  private static final Logger LOG =
      LoggerFactory.getLogger(SetXAttrAction.class);
  public static final String ATT_NAME = "-name";
  public static final String ATT_VALUE = "-value";

  private String srcPath;
  private String attName;
  private String attValue;


  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    if (args.containsKey(FILE_PATH)) {
      this.srcPath = args.get(FILE_PATH);
    }
    if (args.containsKey(ATT_NAME)) {
      this.attName = args.get(ATT_NAME);
    }
    if (args.containsKey(ATT_VALUE)) {
      this.attValue = args.get(ATT_VALUE);
    }
  }

  @Override
  protected void execute() throws Exception {
    if (srcPath == null) {
      throw new IllegalArgumentException("File parameter is missing.");
    }
    if (attName == null) {
      throw new IllegalArgumentException("attName parameter is missing.");
    }
    if (attValue == null) {
      throw new IllegalArgumentException("attValue parameter is missing.");
    }
    if (!dfsClient.exists(srcPath)) {
      throw new ActionException("SetXAttr Action fails, file doesn't exist!");
    }
    LOG.debug("SetXattr path={} name={} value={}", srcPath, attName, attValue);
    appendLog(String.format("SetXattr path=%s name=%s value=%s",
        srcPath, attName, attValue));
    dfsClient.setXAttr(srcPath, attName, attValue.getBytes(),
        EnumSet.of(XAttrSetFlag.CREATE, XAttrSetFlag.REPLACE));
    appendLog("SetXattr Successfully!!");
  }


}
