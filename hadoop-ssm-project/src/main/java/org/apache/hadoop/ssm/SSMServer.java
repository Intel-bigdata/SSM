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
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ssm.protocol.SSMServiceState;
import org.apache.hadoop.ssm.rule.RuleManager;
import org.apache.hadoop.ssm.sql.DBAdapter;
import org.apache.hadoop.ssm.sql.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

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
  private Configuration conf;
  private DistributedFileSystem fs = null;
  private OutputStream outSSMIdFile;
  private List<ModuleSequenceProto> modules = new ArrayList<>();
  static final Path SSM_ID_PATH = new Path("/system/ssm.id");
  public static final Logger LOG = LoggerFactory.getLogger(SSMServer.class);

  private SSMServiceState ssmServiceState = SSMServiceState.SAFEMODE;

  SSMServer(Configuration conf) throws IOException, URISyntaxException {
    this.conf = conf;
    httpServer = new SSMHttpServer(this, conf);
    rpcServer = new SSMRpcServer(this, conf);
    statesManager = new StatesManager(this, conf);
    ruleManager = new RuleManager(this, conf); // TODO: to be replaced
    commandExecutor = new CommandExecutor(this, conf);
    modules.add(statesManager);
    modules.add(ruleManager);
    modules.add(commandExecutor);
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
    if (args != null) {
      // TODO: handle args
    }
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
    String nnRpcAddr = conf.get(
        SSMConfigureKeys.DFS_SSM_NAMENODE_RPCSERVER_KEY);
    if (nnRpcAddr == null) {
      throw new IOException("Can not find NameNode RPC server address. "
          + "Please configure it through '"
          + SSMConfigureKeys.DFS_SSM_NAMENODE_RPCSERVER_KEY + "'.");
    }
    URI rpcURL = new URI(nnRpcAddr);
    this.fs = (DistributedFileSystem) FileSystem.get(rpcURL, conf);
    outSSMIdFile = checkAndMarkRunning();
    if (outSSMIdFile == null) {
      // Exit if there is another one running.
      throw new IOException("Another SSMServer is running");
    }

    // Init and start RPC server and REST server
    rpcServer.start();
    //httpServer.start();

    DBAdapter dbAdapter = getDBAdapter();

    for (ModuleSequenceProto m : modules) {
      m.init(dbAdapter);
    }

    for (ModuleSequenceProto m : modules) {
      m.start();
    }

    // TODO: for simple here, refine it later
    ssmServiceState = SSMServiceState.ACTIVE;
  }

  public SSMServiceState getSSMServiceState() {
    return ssmServiceState;
  }

  public boolean isActive() {
    return ssmServiceState == SSMServiceState.ACTIVE;
  }

  private void stop() throws Exception {
    for (int i = modules.size() - 1 ; i >= 0; i--) {
      modules.get(i).stop();
    }
    httpServer.stop();
    rpcServer.stop();
  }

  /**
   * Waiting services to exit.
   */
  private void join() throws Exception {
    for (int i = modules.size() - 1 ; i >= 0; i--) {
      modules.get(i).join();
    }

    httpServer.join();
    rpcServer.join();
  }

  public void shutdown() {
    try {
      stop();
      join();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public DBAdapter getDBAdapter() throws Exception {
    Connection conn = getDBConnection();
    return new DBAdapter(conn);
  }

  public Connection getDBConnection() throws Exception {
    String dburi = getDBUri();
    Connection conn = Util.createConnection(dburi.toString(), null, null);
    return conn;
  }

  public String getDBUri() throws Exception {
    // TODO: Find and verify the latest SSM DB available,
    // this contains 3 cases:
    //    remote checkpoint / local / create new DB

    String url = conf.get(SSMConfigureKeys.DFS_SSM_DEFAULT_DB_URL_KEY);
    if (url == null) {
      throw new FileNotFoundException("No database found for SSM");
    }
    return url;
  }

  public Configuration getConf() {
    return conf;
  }

  public DFSClient getDFSClient() {
    return fs.getClient();
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
