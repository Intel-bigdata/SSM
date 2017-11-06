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
package org.smartdata.conf;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Console;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

/**
 * SSM related configurations as well as HDFS configurations.
 */
public class SmartConf extends Configuration {
  private static final Logger LOG = LoggerFactory.getLogger(SmartConf.class);

  public SmartConf() {
    Configuration.addDefaultResource("smart-default.xml");
    Configuration.addDefaultResource("smart-site.xml");
  }

  public int getAgentsNumber() {
    String agentConfFile = get(SmartConfKeys.SMART_CONF_DIR_KEY,
            SmartConfKeys.SMART_CONF_DIR_DEFAULT) + "/agents";
    Scanner sc = null;
    try {
      sc = new Scanner(new File(agentConfFile));
    } catch (FileNotFoundException ex) {
      LOG.error("Cannot find the config file: {}!", agentConfFile);
    }
    int num = 0;
    while (sc.hasNextLine()) {
      String host = sc.nextLine().trim();
      if (!host.startsWith("#") && !host.isEmpty()) {
        num++;
      }
    }
    return num;
  }

  public static void main(String[] args) {
    Console console = System.console();
    try {
      Configuration.dumpConfiguration(new SmartConf(), console.writer());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
