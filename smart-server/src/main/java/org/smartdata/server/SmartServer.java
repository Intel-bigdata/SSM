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
import org.smartdata.common.security.JaasLoginUtil;
import org.smartdata.conf.ReconfigurableBase;
import org.smartdata.conf.ReconfigureException;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.common.SmartServiceState;
import org.smartdata.server.engine.CmdletExecutor;
import org.smartdata.server.engine.RuleManager;
import org.smartdata.server.engine.Service;
import org.smartdata.server.engine.SmartRpcServer;
import org.smartdata.server.engine.StatesManager;
import org.smartdata.server.metastore.DBAdapter;
import org.smartdata.server.metastore.MetaUtil;
import org.smartdata.server.utils.GenericOptionsParser;
import org.smartdata.server.web.SmartHttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * From this Smart Storage Management begins.
 */
public class SmartServer extends ReconfigurableBase {
  private StatesManager statesManager;
  private RuleManager ruleManager;
  private CmdletExecutor cmdletExecutor;
  private SmartHttpServer httpServer;
  private SmartRpcServer rpcServer;
  private SmartConf conf;
  private List<Service> modules = new ArrayList<>();
  public static final Logger LOG = LoggerFactory.getLogger(SmartServer.class);

  private SmartServiceState ssmServiceState = SmartServiceState.SAFEMODE;

  SmartServer(SmartConf conf) throws IOException, URISyntaxException {
    this(conf, StartupOption.REGULAR);
  }

  // TODO: will not share the definition
  SmartServer(SmartConf conf, StartupOption startupOption)
      throws IOException, URISyntaxException {
    this.conf = conf;

    checkSecurityAndLogin();

    switch (startupOption) {
      case REGULAR:
        httpServer = new SmartHttpServer(this, conf);
        rpcServer = new SmartRpcServer(this, conf);
        statesManager = new StatesManager(this, conf);
        ruleManager = new RuleManager(this, conf);
        cmdletExecutor = new CmdletExecutor(this, conf);
        modules.add(statesManager);
        modules.add(ruleManager);
        modules.add(cmdletExecutor);
        break;

      // No module started by default
      default:
        break;
    }
  }

  public StatesManager getStatesManager() {
    return statesManager;
  }

  public RuleManager getRuleManager() {
    return ruleManager;
  }

  public CmdletExecutor getCmdletExecutor() {
    return cmdletExecutor;
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
    // TODO: bring back after remove dependency
    //StringUtils.startupShutdownMessage(SmartServer.class, args, LOG);

    if (parseHelpArgument(args, USAGE, System.out, true)) {
      return null;
    }
    // TODO: handle args
    // Parse out some generic args into Configuration.
    GenericOptionsParser hParser = new GenericOptionsParser(conf, args);
    args = hParser.getRemainingArgs();
    // Parse the rest, NN specific args.
    StartupOption startOpt = parseArguments(args);

    switch (startOpt) {
      case REGULAR:
        SmartServer ssm = null;
        try {
          ssm = new SmartServer(conf, startOpt);
          ssm.runSSMDaemons();
        } catch (IOException e) {
          if (ssm != null) {
            ssm.shutdown();
          }
          throw e;
        }
        return ssm;

      case FORMAT:
        LOG.info("Formatting DataBase ...");
        MetaUtil.formatDatabase(conf);
        LOG.info("Formatting DataBase finished successfully!");
        return null;

      default:
        throw new IOException("Not supported start option: " + startOpt);
    }
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
      boolean printGenericCmdletUsage) {
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
      }
    } catch (Exception e) {
      LOG.error("Failed to create SmartServer", e);
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
    // Init and start RPC server and REST server
    rpcServer.start();
    httpServer.start();

    if (conf.getBoolean(SmartConfKeys.DFS_SSM_ENABLED_KEY,
        SmartConfKeys.DFS_SSM_ENABLED_DEFAULT)) {
      startEngines();
      ssmServiceState = SmartServiceState.ACTIVE;
    } else {
      ssmServiceState = SmartServiceState.DISABLED;
    }
  }

  private void startEngines() throws Exception {
    DBAdapter dbAdapter = MetaUtil.getDBAdapter(conf);

    for (Service m : modules) {
      m.init(dbAdapter);
    }

    for (Service m : modules) {
      m.start();
    }
  }

  public void enable() throws IOException {
    if (ssmServiceState == SmartServiceState.DISABLED) {
      try {
        startEngines();
        ssmServiceState = SmartServiceState.ACTIVE;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
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
      join();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public Configuration getConf() {
    return conf;
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

  @VisibleForTesting
  public static StartupOption parseArguments(String args[]) {
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

  @Override
  public void reconfigureProperty(String property, String newVal)
      throws ReconfigureException {
    if (property.equals(SmartConfKeys.DFS_SSM_NAMENODE_RPCSERVER_KEY)) {
      conf.set(property, newVal);
    }
  }

  @Override
  public List<String> getReconfigurableProperties() {
    return Arrays.asList(
        SmartConfKeys.DFS_SSM_NAMENODE_RPCSERVER_KEY
    );
  }
}
