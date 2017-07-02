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
import org.smartdata.SmartContext;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.StatesUpdateService;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class StatesUpdaterServiceFactory {
  public static AbstractService createStatesUpdaterService(Configuration conf,
      SmartContext context, MetaStore metaStore) throws IOException {
    String source = getStatesUpdaterName(conf);
    try {
      Class clazz = Class.forName(source);
      Constructor c = clazz.getConstructor(SmartContext.class, MetaStore.class);
      return (StatesUpdateService) c.newInstance(context, metaStore);
    } catch (ClassNotFoundException | IllegalAccessException
        | InstantiationException | NoSuchMethodException
        | InvocationTargetException e) {
      throw new IOException(e);
    }
  }

  public static String getStatesUpdaterName(Configuration conf) {
    return conf.get(SmartConfKeys.SMART_STATES_UPDATE_SERVICE_KEY,
        SmartConfKeys.SMART_STATES_UPDATE_SERVICE_DEFAULT);
  }
}
