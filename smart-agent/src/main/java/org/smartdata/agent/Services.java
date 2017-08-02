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
package org.smartdata.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.AgentService;
import org.smartdata.SmartContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

public class Services {
  private static final Logger LOG = LoggerFactory.getLogger(Services.class);
  private static Map<String, AgentService> services = new HashMap<>();

  static {
    try {
      ServiceLoader<AgentService> loaded = ServiceLoader.load(AgentService.class);
      for (AgentService service: loaded) {
        services.put(service.getServiceName(), service);
      }
    } catch (ServiceConfigurationError e) {
      LOG.error("Load services failed from factory");
    }
  }

  private Services() {}

  public static void dispatch(AgentService.Message message) throws Exception {
    AgentService service = services.get(message.getServiceName());
    service.execute(message);
  }

  public static void init(SmartContext context) {
    for (Map.Entry<String, AgentService> entry: services.entrySet()) {
      try {
        AgentService service = entry.getValue();
        service.setContext(context);
        service.init();
      } catch (IOException e) {
        LOG.error("Service {} failed to init", entry.getKey());
      }
    }
  }

  public static void start() {
    for (Map.Entry<String, AgentService> entry: services.entrySet()) {
      try {
        AgentService service = entry.getValue();
        service.start();
      } catch (IOException e) {
        LOG.error("Service {} failed to start", entry.getKey());
      }
    }
  }

  public static void stop() {
    for (Map.Entry<String, AgentService> entry: services.entrySet()) {
      try {
        AgentService service = entry.getValue();
        service.stop();
      } catch (IOException e) {
        LOG.error("Service {} failed to stop", entry.getKey());
      }
    }
  }
}
