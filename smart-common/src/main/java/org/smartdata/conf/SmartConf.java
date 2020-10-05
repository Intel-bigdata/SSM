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
import org.smartdata.utils.StringUtil;

import java.io.BufferedInputStream;
import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

/**
 * SSM related configurations as well as HDFS configurations.
 */
public class SmartConf extends Configuration {
  private static final Logger LOG = LoggerFactory.getLogger(SmartConf.class);
  private final List<String> ignoreList;
  private final List<String> coverList;
  private final List<String> lastFetchList = new ArrayList<>();
  // Include hosts configured in conf/agents and
  // hosts added dynamically (by `start-agent.sh --host $host`)
  private Set<String> agentHosts;
  private Set<String> serverHosts;
  Properties props = new Properties();
  String path = System.getProperty("user.dir") + File.separator + "conf"
          + File.separator + "smart.properties";

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
        SmartConfKeys.SMART_WORK_DIR_KEY, SmartConfKeys.SMART_WORK_DIR_DEFAULT);
    tmpDir = tmpDir + (tmpDir.endsWith("/") ? "" : "/");
    ignoreList.add(tmpDir);
    for (String s : ignoreDirs) {
      ignoreList.add(s + (s.endsWith("/") ? "" : "/"));
    }
    for (String s : fetchDirs) {
      coverList.add(s + (s.endsWith("/") ? "" : "/"));
    }

    //get last fetch dirs list
    try {
      InputStream in = new BufferedInputStream(new FileInputStream(path));
      props.load(in);
      String dirs = props.getProperty(SmartConfKeys.SMART_LAST_FETCH_DIRS_KEY);
      String[] lastList = dirs.split(",");
      for (String s : lastList) {
        lastFetchList.add(s + (s.endsWith("/") ? "" : "/"));
      }
      in.close();
    } catch (IOException e) {
      LOG.error("Load smart.properties failed!");
    }

    try {
      this.serverHosts = parseHost("servers", this);
      this.agentHosts = parseHost("agents", this);
    } catch (FileNotFoundException e) {
      // In some unit test, these files may be not given. So such exception is tolerable.
      LOG.warn("Could not get file named servers or agents to parse host.");
    }
  }

  public List<String> getCoverDir() {
    return coverList;
  }

  public List<String> getLastFetchList() {
    return lastFetchList;
  }

  public List<String> getIgnoreDir() {
    return ignoreList;
  }

  /**
   * Update last fetch list to smart.last.fetch.dirs.
   */
  public void setLastFetchList() {
    String writeList = StringUtil.join(",", getCoverDir());
    try {
      OutputStream out = new FileOutputStream(path);
      props.setProperty(SmartConfKeys.SMART_LAST_FETCH_DIRS_KEY, writeList);
      props.store(out, "Update value");
      out.close();
    } catch (IOException e) {
      LOG.error("Update last fetch list failed.");
    }
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

  public Set<String> parseHost(String fileName, SmartConf conf)
      throws FileNotFoundException {
    String hostName = "";
    try {
      InetAddress address = InetAddress.getLocalHost();
      hostName = address.getHostName();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }

    String filePath = conf.get(SmartConfKeys.SMART_CONF_DIR_KEY,
        SmartConfKeys.SMART_CONF_DIR_DEFAULT) + "/" + fileName;
    Scanner sc;
    HashSet<String> hostSet = new HashSet<>();
    sc = new Scanner(new File(filePath));
    while (sc != null && sc.hasNextLine()) {
      String entry = sc.nextLine().trim();
      if (!entry.startsWith("#") && !entry.isEmpty()) {
        if (entry.equals("localhost")) {
          hostSet.add(hostName);
        } else {
          hostSet.add(entry);
        }
      }
    }
    return hostSet;
  }

  /**
   * Add host for newly launched standby server after SSM cluster
   * becomes active.
   */
  public boolean addServerHosts(String hostname) {
    return serverHosts.add(hostname);
  }

  public Set<String> getServerHosts() {
    return serverHosts;
  }

  /**
   * Add host for newly launched agents after SSM cluster
   * becomes active.
   */
  public boolean addAgentHost(String hostname) {
    return agentHosts.add(hostname);
  }

  public Set<String> getAgentHosts() {
    return agentHosts;
  }

  /**
   * Get password for druid by Configuration.getPassword().
   */
  public String getPasswordFromHadoop(String name)
    throws IOException {
    try {
      char[] pw = this.getPassword(name);
      if (pw == null) {
        return null;
      }
      return new String(pw);
    } catch (IOException err) {
      throw new IOException(err.getMessage(), err);
    }
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
