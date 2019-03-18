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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * SSM related configurations as well as HDFS configurations.
 */
public class SmartConf extends Configuration {
  private static final Logger LOG = LoggerFactory.getLogger(SmartConf.class);
  private final List<String> ignoreList;
  private final List<String> coverList;

  public SmartConf() {
    Configuration.addDefaultResource("smart-default.xml");
    Configuration.addDefaultResource("smart-site.xml");

    Collection<String> ignoreDirs = this.getTrimmedStringCollection(
        SmartConfKeys.SMART_IGNORE_DIRS_KEY);
    Collection<String> fetchDirs = this.getTrimmedStringCollection(
        SmartConfKeys.SMART_COVER_DIRS_KEY);
    ignoreList = new ArrayList<>();
    coverList = new ArrayList<>();
    for (String s : ignoreDirs) {
      ignoreList.add(s + (s.endsWith("/") ? "" : "/"));
    }
    for (String s : fetchDirs) {
      coverList.add(s + (s.endsWith("/") ? "" : "/"));
    }
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

  public static void main(String[] args) {
    Console console = System.console();
    try {
      Configuration.dumpConfiguration(new SmartConf(), console.writer());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
