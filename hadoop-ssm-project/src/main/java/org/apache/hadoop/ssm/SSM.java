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
package org.apache.hadoop.ssm;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.util.ExitUtil.terminate;

/**
 * From this Smart Storage Management begins.
 */
public class SSM {
  private StatesManager statesManager;
  private RuleManager ruleManager;
  private CommandExecutor commandExecutor;

  SSM(Configuration conf) {
    statesManager = new StatesManager(this, conf);
    ruleManager = new RuleManager();
    commandExecutor = new CommandExecutor(this, conf);
  }

  public StatesManager getStatesManager() {
    return statesManager;
  }

  public RuleManager getRuleManager() {
    return ruleManager;
  }

  public CommandExecutor getCommandExecutor() {
    return commandExecutor;
  }

  /**
   * Create SSM instance and launch the daemon threads.
   * @param args
   * @param conf
   * @return
   */
  public static SSM createSSM(String[] args, Configuration conf)
      throws Exception {
    SSM ssm = new SSM(conf);
    ssm.runSSMDaemons();
    return ssm;
  }

  private static final String USAGE =
      "Usage: ssm [-help | -foo" +
          " ]\n" +
          "    -help               : Show this usage information.\n" +
          "    -foo                : For example.\n";  // TODO: to be removed

  public static void main(String[] args) {
    if (args.length > 0 && args[0].equals("-help")) {
      System.out.print(USAGE);
      terminate(0);
    }

    Configuration conf = new SSMConfiguration();

    int errorCode = 0;  // if SSM exit normally then the errorCode is 0
    try {
      SSM ssm = createSSM(args, conf);
      if (ssm != null) {
        ssm.join();
      } else {
        errorCode = 1;
      }
    } catch (Exception e) {
      e.printStackTrace();
      terminate(1, e);
    } finally {
      terminate(errorCode);
    }
  }

  /**
   * Bring up all the daemons threads needed.
   * @throws Exception
   */
  public void runSSMDaemons() throws Exception {
    statesManager.start();
    ruleManager.start();
    commandExecutor.start();
  }

  /**
   * Waiting to exit
   */
  private void join() {
  }
}
