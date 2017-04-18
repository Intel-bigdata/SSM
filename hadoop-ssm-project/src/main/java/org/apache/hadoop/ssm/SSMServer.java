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
import org.apache.hadoop.ssm.rule.RuleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.apache.hadoop.util.ExitUtil.terminate;

/**
 * From this Smart Storage Management begins.
 */
public class SSMServer {
  private StatesManager statesManager;
  private RuleManager ruleManager;
  private CommandExecutor commandExecutor;
  private SSMHttpServer httpServer;
  private SSMRpcServer rpcServer;
  private Configuration config;

  public static final Logger LOG = LoggerFactory.getLogger(SSMServer.class);

  SSMServer(Configuration conf) throws IOException {
    config = conf;
    InetSocketAddress addr = InetSocketAddress.createUnresolved("localhost", 9871);
    httpServer = new SSMHttpServer(conf, addr);
    rpcServer = new SSMRpcServer(this, conf);
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
   *
   * @param args
   * @param conf
   * @return
   */
  public static SSMServer createSSM(String[] args, Configuration conf)
      throws Exception {
    SSMServer ssm = new SSMServer(conf);
    ssm.runSSMDaemons();
    return ssm;
  }

  private static final String USAGE =
      "Usage: ssm [-help | -foo" +
          " ]\n" +
          "    -help    : Show this usage information.\n" +
          "    -foo     : For example.\n";// TODO: to be removed

  public static void main(String[] args) {
    if (args.length > 0 && args[0].equals("-help")) {
      System.out.print(USAGE);
      terminate(0);
    }

    Configuration conf = new SSMConfiguration();

    int errorCode = 0;  // if SSM exit normally then the errorCode is 0
    try {
      SSMServer ssm = createSSM(args, conf);
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
   *
   * @throws Exception
   */
  public void runSSMDaemons() throws Exception {
//    httpServer.start();
    rpcServer.start();
    commandExecutor.start();
    statesManager.start();
    ruleManager.start();
  }

  /**
   * Waiting services to exit.
   */
  private void join() throws Exception {
    //httpServer.join();
    rpcServer.join();
  }

  protected InetSocketAddress getRpcServerAddress(Configuration conf) {
//    URI filesystemURI = FileSystem.getDefaultUri(conf);
    return InetSocketAddress.createUnresolved("localhost", 9998);
  }
}
