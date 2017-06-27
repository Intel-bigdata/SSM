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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.common.SmartServiceState;
import org.smartdata.common.security.JaasLoginUtil;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.utils.MetaUtil;
import org.smartdata.server.engine.CmdletExecutor;
import org.smartdata.server.engine.CmdletManager;
import org.smartdata.server.engine.ConfManager;
import org.smartdata.server.engine.RuleManager;
import org.smartdata.server.engine.ServerContext;
import org.smartdata.server.engine.StatesManager;
import org.smartdata.server.utils.GenericOptionsParser;
import org.smartdata.server.web.SmartHttpServer;

import javax.security.auth.Subject;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

/**
 * From this Smart Storage Management begins.
 */
public class SmartServer {
  public static final Logger LOG = LoggerFactory.getLogger(SmartServer.class);

  private SmartHttpServer httpServer;
  private SmartRpcServer rpcServer;
  private ConfManager confMgr;
  private SmartConf conf;
  private SmartEngine engine;
  private ServerContext context;

  private SmartServiceState serviceState = SmartServiceState.SAFEMODE;

  public SmartServer(SmartConf conf) {
    this.conf = conf;
    this.confMgr = new ConfManager(conf);
  }

  public void initWith(StartupOption startupOption) throws Exception {
    checkSecurityAndLogin();

    MetaStore metaStore = MetaUtil.getDBAdapter(conf);
    context = new ServerContext(conf, metaStore);

    if (startupOption == StartupOption.REGULAR) {
      engine = new SmartEngine(context);
      httpServer = new SmartHttpServer(engine, conf);
      rpcServer = new SmartRpcServer(this, conf);
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

  static SmartServer processWith(StartupOption startOption, SmartConf conf) throws Exception {
    if (startOption == StartupOption.FORMAT) {
      LOG.info("Formatting DataBase ...");
      MetaUtil.formatDatabase(conf);
      LOG.info("Formatting DataBase finished successfully!");
    }

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
          + "\tE.g. -D dfs.smart.namenode.rpcserver=hdfs://localhost:43543\n";

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
    return conf.getBoolean(SmartConfKeys.DFS_SSM_SECURITY_ENABLE, false);
  }

  private void checkSecurityAndLogin() throws IOException {
    if (!isSecurityEnabled()) {
      return;
    }
    String keytabFilename = conf.get(SmartConfKeys.DFS_SSM_KEYTAB_FILE_KEY);
    if (keytabFilename == null || keytabFilename.length() == 0) {
      throw new IOException("Running in secure mode, but config doesn't have a keytab");
    }
    File keytabPath = new File(keytabFilename);
    String principal = conf.get(SmartConfKeys.DFS_SSM_KERBEROS_PRINCIPAL_KEY);
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
    boolean enabled = conf.getBoolean(SmartConfKeys.DFS_SSM_ENABLED_KEY,
        SmartConfKeys.DFS_SSM_ENABLED_DEFAULT);

    if (enabled) {
      startEngines();
      serviceState = SmartServiceState.ACTIVE;
    } else {
      serviceState = SmartServiceState.DISABLED;
    }

    rpcServer.start();
    httpServer.start();

//    ZeppelinConfiguration zeppelinConf = ZeppelinConfiguration.create();
//    ZeppelinServer.startZeppelinServer(zeppelinConf);
  }

  private void startEngines() throws Exception {
    engine.init();
    engine.start();
  }

  public void enable() throws IOException {
    if (serviceState == SmartServiceState.DISABLED) {
      try {
        startEngines();
        serviceState = SmartServiceState.ACTIVE;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  public SmartServiceState getSSMServiceState() {
    return serviceState;
  }

  public boolean isActive() {
    return serviceState == SmartServiceState.ACTIVE;
  }

  private void stop() throws Exception {
    if (engine != null) {
      engine.stop();
    }

    try {
      if (httpServer != null) {
        httpServer.stop();
      }
    } catch (Exception e) {
      // ignore to make sure the following services can be closed
    }

    try {
      if (rpcServer != null) {
        rpcServer.stop();
      }
    } catch (Exception e) {
    }
  }

  /**
   * Waiting services to exit.
   */
  /*
  private void join() throws Exception {
    for (int i = modules.size() - 1 ; i >= 0; i--) {
      modules.get(i).join();
    }

    httpServer.join();
    rpcServer.join();
  }*/
  public void shutdown() {
    try {
      stop();
      //join();
    } catch (Exception e) {
      e.printStackTrace();
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
      if (launchWith(args, null) != null) {
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
