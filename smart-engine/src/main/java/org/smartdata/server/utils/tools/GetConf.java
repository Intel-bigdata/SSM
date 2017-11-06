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
package org.smartdata.server.utils.tools;

import com.hazelcast.config.ClasspathXmlConfig;

import java.io.PrintStream;
import java.util.List;

public class GetConf {
  public static final String USAGE =
      "USAGE: GetConf <Option>\n"
          + "\t'Option' can be:\n"
          + "\t\tHelp           Show this help message\n"
          + "\t\tSmartServers   List SmartServers for the cluster(defined in hazelcast.xml)\n";

  public static int getSmartServers(PrintStream p) {
    ClasspathXmlConfig conf = new ClasspathXmlConfig("hazelcast.xml");
    List<String> ret = conf.getNetworkConfig().getJoin().getTcpIpConfig().getMembers();
    for (String s : ret) {
      p.println(s);
    }
    return 0;
  }

  public static void main(String[] args) {
    int exitCode = 0;
    if (args == null || args.length == 0) {
      System.err.println(USAGE);
      System.exit(1);
    }

    try {
      if (args[0].equalsIgnoreCase("SmartServers")) {
        exitCode = getSmartServers(System.out);
      } else if (args[0].equalsIgnoreCase("Help")) {
        System.out.println(USAGE);
      }
    } catch (Throwable t) {
      System.out.println(t.getMessage());
      exitCode = 1;
    }
    System.exit(exitCode);
  }
}
