/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.smartdata.versioninfo;

import java.util.Properties;

public class SsmVersionInfo {
  private static Properties prop = new VersionInfoRead("common").getProperties();

  private SsmVersionInfo() {
  }

  public static String getVersion() {
    return prop.getProperty("version", "Unknown");
  }

  public static String getRevision() {
    return prop.getProperty("revision", "Unknown");
  }

  public static String getTime() {
    return prop.getProperty("date", "Unknown");
  }

  public static String getUser() {
    return prop.getProperty("user", "Unknown");
  }

  public static String getUrl() {
    return prop.getProperty("url", "Unknown");
  }

  public static String getBranch() {
    return prop.getProperty("branch", "Unknown");
  }

  public static String getHadoopVersion() {
    return prop.getProperty("hadoopVersion", "Unknown");
  }

  public static int getHadoopVersionMajor() {
    String str = getHadoopVersion();
    if ("Unknown".equals(str)) {
      return -1;
    }
    String[] vals = str.split("\\.");
    if (vals.length > 0) {
      try {
        return Integer.valueOf(vals[0]);
      } catch (Exception e) {
      }
    }
    return -1;
  }

  public static String infoString() {
    return String.format("SSM %s\n"
        + "Subversion %s\n"
        + "Last commit %s on branch %s\n"
        + "Compiled by %s on %s\n"
        + "Compiled for hadoop %s",
        getVersion(),
        getUrl(),
        getRevision(), getBranch(),
        getUser(), getTime(),
        getHadoopVersion());
  }
}
