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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class VersionInfoRead {

  Properties prop = new Properties();

  protected VersionInfoRead(String component) {
    InputStream in = null;
    String s = component + "-versionInfo.properties";
    try {
      in = Thread.currentThread().getContextClassLoader().getResourceAsStream(s);
      prop.load(in);
    } catch (Exception e) {
      System.out.println(e);
    } finally {
      try {
        in.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
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

  protected String getBranch() {
    return info.prop.getProperty("branch", "Unknown");
  }

  public void printInfo() {
    System.out.println("SSM " + getVersion());
    System.out.println("Subversion " + getUrl());
    System.out.println("Last commit " + getRevision() + " on branch " + getBranch());
    System.out.println("Compiled by " + getUser() + " on " + getTime());
  }

  public static void main(String[] args) {
    VersionInfoRead t = new VersionInfoRead("common");
    t.printInfo();
  }
}
