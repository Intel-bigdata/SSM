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
package org.smartdata.server.cluster;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.conf.SmartConfKeys;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class HazelcastInstanceProvider {
  private static final String CONFIG_FILE = "hazelcast.xml";
  private static HazelcastInstance instance;
  private static final Logger LOG = LoggerFactory.getLogger(HazelcastInstanceProvider.class);

  static {
    String typeKey = "hazelcast.logging.type";
    String loggerType = System.getProperty(typeKey);
    if (loggerType == null) {
      System.setProperty(typeKey, "slf4j");
    }
  }

  private HazelcastInstanceProvider() {}

  public static void addMemberConfig(ClasspathXmlConfig config) {
    NetworkConfig network = config.getNetworkConfig();
    JoinConfig join = network.getJoin();
    String serverConfFile = new Configuration().get(SmartConfKeys.SMART_CONF_DIR_KEY,
        SmartConfKeys.SMART_CONF_DIR_DEFAULT) + "/servers";
    Scanner sc = null;
    try {
      sc = new Scanner(new File(serverConfFile));
    } catch (FileNotFoundException ex) {
      LOG.error("Cannot find the config file: {}!", serverConfFile);
    }
    if (sc != null) {
      while (sc.hasNextLine()) {
        String host = sc.nextLine().trim();
        if (!host.startsWith("#") && !host.isEmpty()) {
          join.getTcpIpConfig().addMember(host);
        }
      }
    }
  }

  public static HazelcastInstance getInstance() {
    if (instance == null) {
      ClasspathXmlConfig config = new ClasspathXmlConfig(CONFIG_FILE);
      addMemberConfig(config);
      instance = Hazelcast.newHazelcastInstance(config);
      Runtime.getRuntime().addShutdownHook(new Thread(){
        @Override public void run() {
          instance.getLifecycleService().shutdown();
        }
      });
    }
    return instance;
  }
}
