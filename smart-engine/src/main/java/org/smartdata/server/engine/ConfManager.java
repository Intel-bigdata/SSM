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
package org.smartdata.server.engine;

import org.smartdata.conf.ReconfigurableBase;
import org.smartdata.conf.ReconfigureException;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;

import java.util.Arrays;
import java.util.List;

public class ConfManager extends ReconfigurableBase {

  private SmartConf conf;

  public ConfManager(SmartConf conf) {
    this.conf = conf;
  }

  @Override
  public void reconfigureProperty(String property, String newVal)
      throws ReconfigureException {
    if (property.equals(SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY)) {
      conf.set(property, newVal);
    }
  }

  @Override
  public List<String> getReconfigurableProperties() {
    return Arrays.asList(
        SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY
    );
  }
}
