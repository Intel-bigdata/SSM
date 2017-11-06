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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class HazelcastInstanceProvider {
  private static final String CONFIG_FILE = "hazelcast.xml";
  private static HazelcastInstance instance;

  static {
    String typeKey = "hazelcast.logging.type";
    String loggerType = System.getProperty(typeKey);
    if (loggerType == null) {
      System.setProperty(typeKey, "slf4j");
    }
  }

  private HazelcastInstanceProvider() {}

  public static HazelcastInstance getInstance() {
    if (instance == null) {
      instance = Hazelcast.newHazelcastInstance(new ClasspathXmlConfig(CONFIG_FILE));
      Runtime.getRuntime().addShutdownHook(new Thread(){
        @Override public void run() {
          instance.getLifecycleService().shutdown();
        }
      });
    }
    return instance;
  }
}
