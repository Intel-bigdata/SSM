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
package org.smartdata.server;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.common.SmartServiceState;
import org.smartdata.server.command.CommandExecutor;
import org.smartdata.server.rule.RuleManager;
import org.smartdata.server.metastore.DBAdapter;
import org.smartdata.server.metastore.DruidPool;
import org.smartdata.server.metastore.Util;
import org.smartdata.server.utils.GenericOptionsParser;
import org.smartdata.server.web.SmartHttpServer;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * From this Smart Storage Management begins.
 */
public class SmartServer {
  private StatesManager statesManager;
  private RuleManager ruleManager;
  private CommandExecutor commandExecutor;
  private SmartHttpServer httpServer;
  private SmartRpcServer rpcServer;
  private SmartConf conf;
  private DistributedFileSystem fs = null;
  private OutputStream outSSMIdFile;
  private List<ModuleSequenceProto> modules = new ArrayList<>();
  static final Path SSM_ID_PATH = new Path("/system/ssm.id");
  private URI namenodeURI;
  public static final Logger LOG = LoggerFactory.getLogger(SmartServer.class);

  private SmartServiceState ssmServiceState = SmartServiceState.SAFEMODE;

  SmartServer(SmartConf conf) throws IOException, URISyntaxException {
    this.conf = conf;
    httpServer = new SmartHttpServer(this, conf);
    rpcServer = new SmartRpcServer(this, conf);
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
  public static SmartServer createSSM(String[] args, SmartConf conf)
      throws Exception {
    if (args == null) {
      args = new String[0];
    }
    StringUtils.startupShutdownMessage(SmartServer.class, args, LOG);
    if (args != null) {
      if (parseHelpArgument(args, USAGE, System.out, true)) {
        return null;
      }
      // TODO: handle args
      // Parse out some generic args into Configuration.
      GenericOptionsParser hParser = new GenericOptionsParser(conf, args);
      args = hParser.getRemainingArgs();
      // Parse the rest, NN specific args.
      StartupOption startOpt = parseArguments(args);
    }
    SmartServer ssm = new SmartServer(conf);
    try {
      ssm.runSSMDaemons();
    } catch (IOException e) {
      ssm.shutdown();
      throw e;
    }
    return ssm;
  }

  private static final String USAGE =
      "Usage: ssm [options]\n"
          + "  -h\n\tShow this usage information.\n\n"
          + "  -D property=value\n"
          + "\tSpecify or overwrite an configure option.\n"
          + "\tE.g. -D dfs.smart.namenode.rpcserver=hdfs://localhost:43543\n";

  public static final Options helpOptions = new Options();
  public static final Option helpOpt = new Option("h", "help", false,
      "get help information");

  static {
    helpOptions.addOption(helpOpt);
  }

  public static boolean parseHelpArgument(String[] args,
      String helpDescription, PrintStream out,
      boolean printGenericCommandUsage) {
    if (args.length == 1) {
      try {
        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = parser.parse(helpOptions, args);
        if (cmdLine.hasOption(helpOpt.getOpt())
            || cmdLine.hasOption(helpOpt.getLongOpt())) {
          // should print out the help information
          out.println(helpDescription + "\n");
          return true;
        }
      } catch (ParseException pe) {
        LOG.warn("Parse help exception", pe);
        return false;
      }
    }
    return false;
  }

  public static void main(String[] args) {
    SmartConf conf = new SmartConf();

    int errorCode = 0;  // if SSM exit normally then the errorCode is 0
    try {
      SmartServer ssm = createSSM(args, conf);
      if (ssm != null) {
        //ssm.join();
        // TODO: block now,  to be refined
        while (true) {
          Thread.sleep(1000);
        }
      } else {
        errorCode = 1;
      }
    } catch (Exception e) {
      System.out.println("\n");
      e.printStackTrace();
      System.exit(1);
    } finally {
      System.exit(errorCode);
    }
  }

  /**
   * Bring up all the daemons threads needed.
   *
   * @throws Exception
   */
  public void runSSMDaemons() throws Exception {
    String nnRpcAddr = null;

    String[] rpcAddrKeys = {
        SmartConfKeys.DFS_SSM_NAMENODE_RPCSERVER_KEY, // Keep it first
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY,
        DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY
    };

    String[] nnRpcAddrs = new String[rpcAddrKeys.length];

    int lastNotNullIdx = 0;
    for (int index = 0; index < rpcAddrKeys.length; index++) {
      nnRpcAddrs[index] = conf.get(rpcAddrKeys[index]);
      lastNotNullIdx = nnRpcAddrs[index] == null ? lastNotNullIdx : index;
      nnRpcAddr = nnRpcAddr == null ? nnRpcAddrs[index] : nnRpcAddr;
    }

    if (nnRpcAddr == null) {
      throw new IOException("Can not find NameNode RPC server address. "
          + "Please configure it through '"
          + SmartConfKeys.DFS_SSM_NAMENODE_RPCSERVER_KEY + "'.");
    }

    if (lastNotNullIdx == 0 && rpcAddrKeys.length > 1) {
      conf.set(rpcAddrKeys[1], nnRpcAddr);
    }

    namenodeURI = new URI(nnRpcAddr);
    this.fs = (DistributedFileSystem) FileSystem.get(namenodeURI, conf);
    outSSMIdFile = checkAndMarkRunning();
    if (outSSMIdFile == null) {
      // Exit if there is another one running.
      throw new IOException("Another SmartServer is running");
    }

    // Init and start RPC server and REST server
    rpcServer.start();
    httpServer.start();

    DBAdapter dbAdapter = getDBAdapter();

    for (ModuleSequenceProto m : modules) {
      m.init(dbAdapter);
    }

    for (ModuleSequenceProto m : modules) {
      m.start();
    }

    // TODO: for simple here, refine it later
    ssmServiceState = SmartServiceState.ACTIVE;
  }

  public URI getNamenodeURI() {
    return namenodeURI;
  }

  public SmartServiceState getSSMServiceState() {
    return ssmServiceState;
  }

  public boolean isActive() {
    return ssmServiceState == SmartServiceState.ACTIVE;
  }

  public void stop() throws Exception {
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
      if (outSSMIdFile != null) {
        outSSMIdFile.close();
        outSSMIdFile = null;
      }
      join();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public DBAdapter getDBAdapter() throws Exception {
    String fileName = "druid.xml";
    URL urlPoolConf = getClass().getResource(fileName);
    if (urlPoolConf != null) {
      LOG.info("Using pool configure file: " + urlPoolConf.getFile());
      Properties p = new Properties();
      p.loadFromXML(getClass().getResourceAsStream(fileName));
      return new DBAdapter(new DruidPool(p));
    } else {
      LOG.info(fileName + " NOT found.");
    }

    // TODO: keep it now for testing, remove it later.
    Connection conn = getDBConnection();
    return new DBAdapter(conn);
  }

  public Connection getDBConnection() throws Exception {
    String dburi = getDBUri();
    LOG.info("Database file URI = " + dburi);
    Connection conn = Util.createConnection(dburi.toString(), null, null);
    return conn;
  }

  public String getDBUri() throws Exception {
    // TODO: Find and verify the latest SSM DB available,
    // this contains 3 cases:
    //    remote checkpoint / local / create new DB

    String url = conf.get(SmartConfKeys.DFS_SSM_DEFAULT_DB_URL_KEY);
    if (url == null) {
      LOG.warn("No database specified for SSM, "
          + "will use a default one instead.");
    }
    return url != null ? url : getDefaultSqliteDB() ;
  }

  /**
   * This default behavior provided here is mainly for convenience.
   * @return
   */
  private String getDefaultSqliteDB() throws Exception {
    String absFilePath = System.getProperty("user.home")
        + "/smart-test-default.db";
    File file = new File(absFilePath);
    if (file.exists()) {
      return Util.SQLITE_URL_PREFIX + absFilePath;
    }
    Connection conn = Util.createSqliteConnection(absFilePath);
    Util.initializeDataBase(conn);
    conn.close();
    return Util.SQLITE_URL_PREFIX + absFilePath;
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

  @VisibleForTesting
  public static StartupOption parseArguments(String args[]) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i];
      if (StartupOption.FORMAT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FORMAT;
      } else if (StartupOption.CHECKPOINT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.CHECKPOINT;
      } else {
        return null;
      }
    }
    return startOpt;
  }
}
