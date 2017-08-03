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
package org.smartdata.server.engine.cmdlet;

import org.apache.hadoop.conf.Configuration;
import org.smartdata.SmartContext;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metastore.ActionPreProcessService;
import org.smartdata.metastore.MetaStore;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class ActionSchedulerServiceFactory {
  public static List<ActionPreProcessService> createServices(Configuration conf,
      SmartContext context, MetaStore metaStore, boolean allMustSuccess) throws IOException {
    List<ActionPreProcessService> services = new ArrayList<>();
    String[] serviceNames = getServiceNames(conf);
    for (String name : serviceNames) {
      try {
        Class clazz = Class.forName(name);
        Constructor c = clazz.getConstructor(SmartContext.class, MetaStore.class);
        services.add((ActionPreProcessService) c.newInstance(context, metaStore));
      } catch (ClassNotFoundException | IllegalAccessException
          | InstantiationException | NoSuchMethodException
          | InvocationTargetException e) {
        if (allMustSuccess) {
          throw new IOException(e);
        } else {
          // ignore this
        }
      }
    }
    return services;
  }

  public static String[] getServiceNames(Configuration conf) {
    return conf.getTrimmedStrings(SmartConfKeys.SMART_ACTION_SCHEDULER_SERVICE_KEY,
        SmartConfKeys.SMART_ACTION_SCHEDULER_SERVICE_DEFAULT);
  }
}
