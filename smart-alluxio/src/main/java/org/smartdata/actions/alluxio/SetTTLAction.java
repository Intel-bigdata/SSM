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

public class SetTTLAction extends AlluxioAction {

  private long ttl;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.actionType = AlluxioActionType.SetTTL;
    this.ttl = Long.valueOf(args.get("TTL"));
  }

  @Override
  protected void execute() {
    try {
      LOG.info("Executing Alluxio action: SetTTLAction, path:" + uri.toString());
      SetAttributeOptions options = SetAttributeOptions.defaults().setTtl(ttl);
      alluxioFs.setAttribute(uri, options);
      LOG.info("File " + uri + " was successfully set TTL to " + ttl + ".");
    } catch (Exception e) {
      actionStatus.end();
      actionStatus.setSuccessful(false);
      throw new RuntimeException(e);
    }
  }
}
