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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.util.Properties;

public class VersionInfoRead {

  Properties prop = new Properties();

  protected VersionInfoRead(String component) {
    String s = this.getClass().getResource("/").getPath() + component + "-versionInfo.properties";
    try {
      FileReader v = new FileReader(s);
      InputStream in = new BufferedInputStream(new FileInputStream(s));
      prop.load(in);
      in.close();
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  private static VersionInfoRead info = new VersionInfoRead("common");

  protected String getVersion() {
    return info.prop.getProperty("version", "Unknown");
  }

  protected String getRevision() {
    return info.prop.getProperty("revision", "Unknown");
  }

  protected String getTime() {
    return info.prop.getProperty("date", "Unknown");
  }

  protected String getUser() {
    return info.prop.getProperty("user", "Unknown");
  }

  protected String getUrl() {
    return info.prop.getProperty("url", "Unknown");
  }

  public void printInfo() {
    System.out.println("SSM " + getVersion());
    System.out.println("Subversion " + getUrl() + " -r " + getRevision());
    System.out.println("Compiled by " + getUser() + " on " + getTime());
  }

  public static void main(String[] args) {
    VersionInfoRead t = new VersionInfoRead("common");
    t.printInfo();
  }
}
