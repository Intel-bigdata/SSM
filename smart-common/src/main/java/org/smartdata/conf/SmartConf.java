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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

/**
 * SSM related configurations as well as HDFS configurations.
 */
public class SmartConf extends Configuration {
  private static final Logger LOG = LoggerFactory.getLogger(SmartConf.class);
  private final List<String> ignoreList;
  private final List<String> coverList;
  private Set<String> agentHosts;
  private Set<String> serverHosts;

  public SmartConf() {
    Configuration.addDefaultResource("smart-default.xml");
    Configuration.addDefaultResource("smart-site.xml");

    Collection<String> ignoreDirs = this.getTrimmedStringCollection(
        SmartConfKeys.SMART_IGNORE_DIRS_KEY);
    Collection<String> fetchDirs = this.getTrimmedStringCollection(
        SmartConfKeys.SMART_COVER_DIRS_KEY);

    ignoreList = new ArrayList<>();
    coverList = new ArrayList<>();
    String tmpDir = this.get(
        SmartConfKeys.SMART_TMP_DIR_KEY, SmartConfKeys.SMART_TMP_DIR_DEFAULT);
    tmpDir = tmpDir + (tmpDir.endsWith("/") ? "" : "/");
    ignoreList.add(tmpDir);
    for (String s : ignoreDirs) {
      ignoreList.add(s + (s.endsWith("/") ? "" : "/"));
    }
    for (String s : fetchDirs) {
      coverList.add(s + (s.endsWith("/") ? "" : "/"));
    }

    this.serverHosts = init("servers", this);
    this.agentHosts = init("agents", this);
  }

  public List<String> getCoverDir() {
    return coverList;
  }

  public List<String> getIgnoreDir() {
    return ignoreList;
  }

  public void setCoverDir(ArrayList<String> fetchDirs) {
    coverList.clear();
    for (String s : fetchDirs) {
      coverList.add(s + (s.endsWith("/") ? "" : "/"));
    }
  }

  public void setIgnoreDir(ArrayList<String> ignoreDirs) {
    ignoreList.clear();
    for (String s : ignoreDirs) {
      ignoreList.add(s + (s.endsWith("/") ? "" : "/"));
    }
  }

  public Set<String> init(String fileName, SmartConf conf) {
    String hostName = "";
    try {
      InetAddress address = InetAddress.getLocalHost();
      hostName = address.getHostName();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }

    String filePath = conf.get(SmartConfKeys.SMART_CONF_DIR_KEY,
        SmartConfKeys.SMART_CONF_DIR_DEFAULT) + "/" + fileName;
    Scanner sc = null;
    HashSet<String> hosts = new HashSet<>();
    try {
      sc = new Scanner(new File(filePath));
    } catch (FileNotFoundException ex) {
      ex.printStackTrace();
    }

    while (sc != null && sc.hasNextLine()) {
      String host = sc.nextLine().trim();
      if (!host.startsWith("#") && !host.isEmpty()) {
        if (host.equals("localhost")) {
          hosts.add(hostName);
        } else {
          hosts.add(host);
        }
      }
    }
    return hosts;
  }

  public Set<String> getServerHosts() {
    return serverHosts;
  }

  public Set<String> getAgentHosts() {
    return agentHosts;
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
