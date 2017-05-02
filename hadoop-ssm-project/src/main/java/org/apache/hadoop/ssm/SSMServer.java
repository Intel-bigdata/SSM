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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ssm.rule.RuleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

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
  private DistributedFileSystem fs = null;
  private OutputStream out;
  static final Path SSM_ID_PATH = new Path("/system/ssm.id");
  public static final Logger LOG = LoggerFactory.getLogger(SSMServer.class);

  SSMServer(Configuration conf) throws IOException, URISyntaxException {
    String rpcAddr = conf.get("dfs.ssm.namenode.rpcserver");
    URI rpcURL = new URI(rpcAddr);
    this.fs = (DistributedFileSystem) FileSystem.get(rpcURL, conf);
    config = conf;
    InetSocketAddress addr = InetSocketAddress.createUnresolved("localhost", 9871);
    httpServer = new SSMHttpServer(conf, addr);
    rpcServer = new SSMRpcServer(this, conf);
    statesManager = new StatesManager(this, conf);
    ruleManager = new RuleManager(this, conf, null); // TODO: to be replaced
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
    ssm.out = ssm.checkAndMarkRunning();
    if (ssm.out == null) {
      // Exit if there is another one running.
      throw new IOException("Another SSMServer is running");
    }
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
    String[] strings = conf.get("dfs.ssm.rpcserver").split(":");
    return InetSocketAddress.createUnresolved(strings[strings.length - 2]
        , Integer.parseInt(strings[strings.length - 1]));
  }

  private OutputStream checkAndMarkRunning() throws IOException {
    try {
      if (fs.exists(SSM_ID_PATH)) {
        // try appending to it so that it will fail fast if another balancer is
        // running.
        IOUtils.closeStream(fs.append(SSM_ID_PATH));
        fs.delete(SSM_ID_PATH, true);
      }
      final FSDataOutputStream fsout = fs.create(SSM_ID_PATH, false);
      // mark balancer idPath to be deleted during filesystem closure
      fs.deleteOnExit(SSM_ID_PATH);
      fsout.writeBytes(InetAddress.getLocalHost().getHostName());
      fsout.hflush();
      return fsout;
    } catch (RemoteException e) {
      if (AlreadyBeingCreatedException.class.getName().equals(e.getClassName())) {
        return null;
      } else {
        throw e;
      }
    }
  }

}
