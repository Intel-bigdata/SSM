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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.zeppelin.server.SmartZeppelinServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.smartdata.SmartServiceState;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.utils.MetaStoreUtils;
import org.smartdata.server.engine.CmdletManager;
import org.smartdata.server.engine.ConfManager;
import org.smartdata.server.engine.RuleManager;
import org.smartdata.server.engine.ServerContext;
import org.smartdata.server.engine.ServiceMode;
import org.smartdata.server.engine.StatesManager;
import org.smartdata.server.engine.cmdlet.agent.AgentMaster;
import org.smartdata.server.utils.GenericOptionsParser;
import org.smartdata.tidb.LaunchDB;
import org.smartdata.tidb.PdServer;
import org.smartdata.tidb.TidbServer;
import org.smartdata.utils.JaasLoginUtil;

import javax.security.auth.Subject;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.util.Scanner;

/**
 * From this Smart Storage Management begins.
 */
public class SmartServer {
  public static final Logger LOG = LoggerFactory.getLogger(SmartServer.class);

  private ConfManager confMgr;
  private SmartConf conf;
  private SmartEngine engine;
  private ServerContext context;
  private boolean enabled;

  private SmartRpcServer rpcServer;
  private SmartZeppelinServer zeppelinServer;

  static {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  public SmartServer(SmartConf conf) {
    this.conf = conf;
    this.confMgr = new ConfManager(conf);
    this.enabled = false;
  }

  public void initWith(StartupOption startupOption) throws Exception {
    checkSecurityAndLogin();

    MetaStore metaStore = MetaStoreUtils.getDBAdapter(conf);
    context = new ServerContext(conf, metaStore);
    initServiceMode(conf);
    if (startupOption == StartupOption.REGULAR) {
      engine = new SmartEngine(context);
      rpcServer = new SmartRpcServer(this, conf);
      zeppelinServer = new SmartZeppelinServer(conf, engine);
    }
  }

  public StatesManager getStatesManager() {
    return engine.getStatesManager();
  }

  public RuleManager getRuleManager() {
    return engine.getRuleManager();
  }

  public CmdletManager getCmdletManager() {
    return engine.getCmdletManager();
  }

  public MetaStore getMetaStore() {
    return this.context.getMetaStore();
  }

  public ServerContext getContext() {
    return this.context;
  }

  public static StartupOption processArgs(String[] args, SmartConf conf) throws Exception {
    if (args == null) {
      args = new String[0];
    }

    if (parseHelpArgument(args, USAGE, System.out, true)) {
      return null;
    }

    GenericOptionsParser hParser = new GenericOptionsParser(conf, args);
    args = hParser.getRemainingArgs();

    StartupOption startOpt = parseArguments(args);

    return startOpt;
  }

  public static void startDB(SmartConf conf) throws InterruptedException, IOException {
    if (conf.getAgentsNumber() != 0) {
      AgentMaster agentMaster = AgentMaster.getAgentMaster(conf);
      String host = agentMaster.getAgentMasterHost();
      InetAddress address = InetAddress.getByName(host);
      String ip = address.getHostAddress();
      String pdArgs = new String("--client-urls=http://" + ip + ":2379 --data-dir=pd --log-file=logs/pd.log");
      PdServer pdServer = new PdServer(pdArgs);
      Thread pdThread = new Thread(pdServer);
      pdThread.start();
      while (!pdServer.isReady() || !agentMaster.isAgentRegisterReady()) {
        Thread.sleep(100);
      }
      LOG.info("Pd server is ready.");
      agentMaster.sendLaunchTikvMessage();
      while (!agentMaster.isAlreadyLaunchedTikv()) {
        Thread.sleep(100);
      }
      LOG.info("Tikv server is ready.");
      String tidbArgs = new String("--store=tikv --path=" + host + ":2379 --lease=1s --log-file=logs/tidb.log");
      TidbServer tidbServer = new TidbServer(tidbArgs);
      Thread tidbThread = new Thread(tidbServer);
      tidbThread.start();
      while (!tidbServer.isReady()) {
        Thread.sleep(100);
      }
      LOG.info("Tidb server is ready.");
    } else {
      LaunchDB launchDB = new LaunchDB();
      Thread db = new Thread(launchDB);
      LOG.info("Starting Pd, Tikv and Tidb..");
      db.start();
      while (!launchDB.isCompleted()) {
        Thread.sleep(100);
      }
    }
  }

  static SmartServer processWith(StartupOption startOption, SmartConf conf) throws Exception {
    if (isTidbEnabled(conf)) {
      startDB(conf);
    }

    if (startOption == StartupOption.FORMAT) {
      LOG.info("Formatting DataBase ...");
      MetaStoreUtils.formatDatabase(conf);
      LOG.info("Formatting DataBase finished successfully!");
      return null;
    }

    MetaStoreUtils.checkTables(conf);

    SmartServer ssm = new SmartServer(conf);
    try {
      ssm.initWith(startOption);
      ssm.run();
      return ssm;
    } catch (Exception e){
      ssm.shutdown();
      throw e;
    }
  }

  private static final String USAGE =
      "Usage: ssm [options]\n"
          + "  -h\n\tShow this usage information.\n\n"
          + "  -D property=value\n"
          + "\tSpecify or overwrite an configure option.\n"
          + "\tE.g. -D smart.dfs.namenode.rpcserver=hdfs://localhost:43543\n";

  private static final Options helpOptions = new Options();
  private static final Option helpOpt = new Option("h", "help", false,
      "get help information");

  static {
    helpOptions.addOption(helpOpt);
  }

  private static boolean parseHelpArgument(String[] args,
      String helpDescription, PrintStream out, boolean printGenericCmdletUsage) {
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
        //LOG.warn("Parse help exception", pe);
        return false;
      }
    }
    return false;
  }

  private boolean isSecurityEnabled() {
    return conf.getBoolean(SmartConfKeys.SMART_SECURITY_ENABLE, false);
  }

  private static boolean isTidbEnabled(SmartConf conf) {
    return conf.getBoolean(SmartConfKeys.SMART_TIDB_ENABLED,SmartConfKeys.SMART_TIDB_ENABLED_DEFAULT);
  }

  private void checkSecurityAndLogin() throws IOException {
    if (!isSecurityEnabled()) {
      return;
    }
    String keytabFilename = conf.get(SmartConfKeys.SMART_SERVER_KEYTAB_FILE_KEY);
    if (keytabFilename == null || keytabFilename.length() == 0) {
      throw new IOException("Running in secure mode, but config doesn't have a keytab");
    }
    File keytabPath = new File(keytabFilename);
    String principal = conf.get(SmartConfKeys.SMART_SERVER_KERBEROS_PRINCIPAL_KEY);
    Subject subject = null;
    try {
      subject = JaasLoginUtil.loginUsingKeytab(principal, keytabPath);
    } catch (IOException e) {
      LOG.error("Fail to login using keytab. " + e);
    }
    LOG.info("Login successful for user: "
        + subject.getPrincipals().iterator().next());
  }

  /**
   * Bring up all the daemon threads needed.
   *
   * @throws Exception
   */
  private void run() throws Exception {
    boolean enabled = conf.getBoolean(SmartConfKeys.SMART_DFS_ENABLED,
        SmartConfKeys.SMART_DFS_ENABLED_DEFAULT);

    if (enabled) {
      startEngines();
    }

    rpcServer.start();

    if (zeppelinServer != null) {
      zeppelinServer.start();
    }
  }

  private void startEngines() throws Exception {
    enabled = true;
    engine.init();
    engine.start();
  }

  public void enable() throws IOException {
    if (getSSMServiceState() == SmartServiceState.DISABLED) {
      try {
        startEngines();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  public SmartServiceState getSSMServiceState() {
    if (!enabled) {
      return SmartServiceState.DISABLED;
    } else if (!engine.inSafeMode()) {
      return SmartServiceState.ACTIVE;
    } else {
      return SmartServiceState.SAFEMODE;
    }
  }

  public boolean isActive() {
    return getSSMServiceState() == SmartServiceState.ACTIVE;
  }

  private void stop() throws Exception {
    if (engine != null) {
      engine.stop();
    }

    if (zeppelinServer != null) {
      zeppelinServer.stop();
    }

    try {
      if (rpcServer != null) {
        rpcServer.stop();
      }
    } catch (Exception e) {
    }
  }

  public void shutdown() {
    try {
      stop();
      //join();
    } catch (Exception e) {
      LOG.error("SmartServer shutdown error", e);
    }
  }

  private enum StartupOption {
    FORMAT("-format"),
    REGULAR("-regular");

    private String name;

    StartupOption(String arg) {
      this.name = arg;
    }

    public String getName() {
      return name;
    }
  }

  private void initServiceMode(SmartConf conf) {
    String serviceModeStr = conf.get(SmartConfKeys.SMART_SERVICE_MODE_KEY,
        SmartConfKeys.SMART_SERVICE_MODE_DEFAULT);
    try {
      context.setServiceMode(ServiceMode.valueOf(serviceModeStr.trim().toUpperCase()));
    } catch (IllegalStateException e) {
      String errorMsg = "Illegal service mode '" + serviceModeStr + "' set in property: "+
          SmartConfKeys.SMART_SERVICE_MODE_KEY + "!";
      LOG.error(errorMsg);
      throw e;
    }
    LOG.info("Initialized service mode: "+ context.getServiceMode().getName() + ".");
  }

  private static StartupOption parseArguments(String args[]) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i];
      if (StartupOption.FORMAT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FORMAT;
      } else if (StartupOption.REGULAR.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.REGULAR;
      }
    }
    return startOpt;
  }

  public static SmartServer launchWith(SmartConf conf) throws Exception {
    return launchWith(null, conf);
  }

  public static SmartServer launchWith(String[] args, SmartConf conf) throws Exception {
    if (conf == null) {
      conf = new SmartConf();
    }

    StartupOption startOption = processArgs(args, conf);
    if (startOption == null) {
      return null;
    }
    return processWith(startOption, conf);
  }

  public static void main(String[] args) {
    int errorCode = 0;  // if SSM exit normally then the errorCode is 0
    try {
      final SmartServer inst = launchWith(args, null);
      if (inst != null) {
        Runtime.getRuntime().addShutdownHook(new Thread(){
          @Override
          public void run() {
            LOG.info("Shutting down SmartServer ... ");
            try {
              inst.shutdown();
             } catch (Exception e) {
               LOG.error("Error while stopping servlet container", e);
             }
             LOG.info("SmartServer was down.");
           }
         });
        //Todo: when to break
        while (true) {
          Thread.sleep(1000);
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to create SmartServer", e);
      System.exit(1);
    } finally {
      System.exit(errorCode);
    }
  }
}
