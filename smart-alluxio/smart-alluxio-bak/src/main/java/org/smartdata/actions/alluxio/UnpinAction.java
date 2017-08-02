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
package org.smartdata.actions.alluxio;

import java.util.Map;

import alluxio.client.file.options.SetAttributeOptions;

public class UnpinAction extends AlluxioAction {

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.actionType = AlluxioActionType.UNPIN;
  }

  @Override
  protected void execute() throws Exception {
    LOG.info("Executing Alluxio action: UnpinAction, path:" + uri.toString());
    SetAttributeOptions options = SetAttributeOptions.defaults().setPinned(false);
    alluxioFs.setAttribute(uri, options);
    LOG.info("File " + uri + " was successfully unpinned.");
  }
}
