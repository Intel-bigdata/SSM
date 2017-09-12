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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.AbstractService;
import org.smartdata.SmartConstants;
import org.smartdata.SmartContext;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metastore.ActionSchedulerService;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.StatesUpdateService;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class AbstractServiceFactory {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractServiceFactory.class);

  public static AbstractService createStatesUpdaterService(Configuration conf,
      ServerContext context, MetaStore metaStore) throws IOException {
    String source = getStatesUpdaterName(getServiceMode(context.getConf()));
    try {
      Class clazz = Class.forName(source);
      Constructor c = clazz.getConstructor(SmartContext.class, MetaStore.class);
      return (StatesUpdateService) c.newInstance(context, metaStore);
    } catch (ClassNotFoundException | IllegalAccessException
        | InstantiationException | NoSuchMethodException
        | InvocationTargetException | NullPointerException e) {
      throw new IOException(e);
    }
  }

  public static String getStatesUpdaterName(ServiceMode mode) {
    switch (mode) {
    case HDFS:
      return SmartConstants.SMART_HDFS_STATES_UPDATE_SERVICE_IMPL;
    case ALLUXIO:
      return SmartConstants.SMART_ALLUXIO_STATES_UPDATE_SERVICE_IMPL;
    default:
      return SmartConstants.SMART_HDFS_STATES_UPDATE_SERVICE_IMPL;
    }
  }

  public static List<ActionSchedulerService> createActionSchedulerServices(Configuration conf,
      SmartContext context, MetaStore metaStore, boolean allMustSuccess) throws IOException {
    List<ActionSchedulerService> services = new ArrayList<>();
    String[] serviceNames = getActionSchedulerNames(conf, getServiceMode(context.getConf()));
    for (String name : serviceNames) {
      try {
        if (!name.isEmpty()) {
          Class clazz = Class.forName(name);
          Constructor c = clazz.getConstructor(SmartContext.class, MetaStore.class);
          services.add((ActionSchedulerService) c.newInstance(context, metaStore));
        }
      } catch (ClassNotFoundException | IllegalAccessException
          | InstantiationException | NoSuchMethodException
          | InvocationTargetException | NullPointerException e) {
        if (allMustSuccess) {
          throw new IOException(e);
        } else {
          LOG.warn("Error while create action scheduler service '" + name + "'.", e);
        }
      }
    }
    return services;
  }

  public static String[] getActionSchedulerNames(Configuration conf, ServiceMode mode) {
    switch (mode) {
      case HDFS:
        return SmartConstants.SMART_HDFS_ACTION_SCHEDULER_SERVICE_IMPL.trim().split("\\s*,\\s*");
      case ALLUXIO:
        return SmartConstants.SMART_ALLUXIO_ACTION_SCHEDULER_SERVICE_IMPL.trim().split("\\s*,\\s*");
      default:
        return SmartConstants.SMART_HDFS_ACTION_SCHEDULER_SERVICE_IMPL.trim().split("\\s*,\\s*");
    }
  }

  public static ServiceMode getServiceMode(SmartConf conf) {
    String serviceModeStr = conf.get(SmartConfKeys.SMART_SERVICE_MODE_KEY,
        SmartConfKeys.SMART_SERVICE_MODE_DEFAULT);
    try {
      return ServiceMode.valueOf(serviceModeStr.trim().toUpperCase());
    } catch (IllegalStateException e) {
      String errorMsg = "Illegal service mode '" + serviceModeStr + "' set in property: "+
          SmartConfKeys.SMART_SERVICE_MODE_KEY + "!";
      LOG.error(errorMsg);
      throw e;
    }
  }

}
