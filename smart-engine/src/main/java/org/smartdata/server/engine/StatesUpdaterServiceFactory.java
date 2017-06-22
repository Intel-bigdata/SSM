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

import org.apache.hadoop.conf.Configuration;
import org.smartdata.AbstractService;

import java.io.IOException;

public class StatesUpdaterServiceFactory {
  private static final String STATES_UPDATER_SERVICES_KEY = "dfs.smartdata.states.updater";
  private static final String STATES_UPDATER_SERVICES_DEFAULT = "org.smartdata.hdfs.HdfsStatesUpdaterService";

  public static AbstractService createStatesUpdaterService(Configuration conf)
      throws IOException {
    String source = conf.get(STATES_UPDATER_SERVICES_KEY, STATES_UPDATER_SERVICES_DEFAULT);
    try {
      Class clazz = Class.forName(source);
      return (AbstractService) clazz.newInstance();
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new IOException(e);
    }
  }
}
