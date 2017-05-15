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
package org.apache.hadoop.ssm.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.ssm.SSMConfiguration;
import org.apache.hadoop.ssm.protocol.SSMClient;
import org.apache.hadoop.ssm.rule.RuleInfo;
import org.apache.hadoop.ssm.rule.RuleState;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class RuleCommands {
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(SubmitRule.class, "submitrule");
    factory.addClass(ListRules.class, "listrules");
  }

  private static SSMClient newSSMClient(Command cmd) throws IOException {
    Configuration conf = cmd.getConf();
    if (conf == null) {
      conf = new SSMConfiguration();
    }

    //System.out.println(conf.get(SSMConfigureKeys.DFS_SSM_RPC_ADDRESS_KEY));
    SSMClient client = new SSMClient(conf);
    return client;
  }

  public static class SubmitRule extends Command {
    public static final String NAME = "submitrule";
    public static final String USAGE =
        "rule_file_path [initial_state]";
    public static final String DESCRIPTION = "Submit a rule into SSM. "
        + "Default value for initial_state' is ACTIVE.";

    public int doSubmit(String[] args) throws IOException {
      SSMClient client = newSSMClient(this);
      File file = new File(args[0]);
      if (!file.exists() || file.isDirectory() || !file.canRead()) {
        throw new IOException("Invalid rule file path: " + args[0]);
      }
      if (file.length() >= 2 * 1024L * 1024 * 1024) {
        throw new IOException("File size is too big (>= 2BG)");
      }
      int len = (int)file.length();
      int nreaded = 0;
      FileInputStream in = new FileInputStream(file);
      byte[] con = new byte[len];
      while (nreaded < len) {
        nreaded += in.read(con, nreaded, len - nreaded);
      }
      in.close();
      String ruleText = new String(con);
      System.out.println("Rule text:\n" + ruleText);

      RuleState state =
          args.length >= 2 ? RuleState.fromName(args[1]) : RuleState.ACTIVE;
      if (state == null) {
        throw new IOException("Invalid initial state");
      }
      long ruleId = client.submitRule(ruleText, state);
      System.out.println("\nCompleted successfully! RuleID = " + ruleId);
      return 0;
    }

    public String getCommandName() {
      return NAME;
    }

    protected void run(Path path) throws IOException {
      throw new IOException("Method not implemented");
    }

    public int run(String[] argv) {
      //System.out.println("Args = " + argv.toString());
      try {
        return doSubmit(argv);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return -1;
    }
  }

  public static class ListRules extends Command {
    public static final String NAME = "listrules";
    public static final String USAGE = "";
    public static final String DESCRIPTION = "List rules in SSM";

    public int doList(String[] args) throws IOException {
      SSMClient client = newSSMClient(this);
      List<RuleInfo> infos = client.listRulesInfo();
      for (RuleInfo info : infos) {
        System.out.println(info);
      }
      System.out.println("Totally " + infos.size() + " in SSM.");
      return 0;
    }

    public String getCommandName() {
      return NAME;
    }

    protected void run(Path path) throws IOException {
      throw new IOException("Method not implemented");
    }

    public int run(String[] argv) {
      //System.out.println("Args = " + argv.toString());
      try {
        return doList(argv);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return -1;
    }
  }

}
