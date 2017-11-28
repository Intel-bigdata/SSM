/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.server;

import com.sun.jersey.api.core.ApplicationAdapter;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.apache.commons.lang.StringUtils;
import org.apache.shiro.web.env.EnvironmentLoaderListener;
import org.apache.shiro.web.servlet.ShiroFilter;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.conf.ZeppelinConfiguration.ConfVars;
import org.apache.zeppelin.dep.DependencyResolver;
import org.apache.zeppelin.helium.Helium;
import org.apache.zeppelin.helium.HeliumApplicationFactory;
import org.apache.zeppelin.helium.HeliumVisualizationFactory;
import org.apache.zeppelin.interpreter.InterpreterFactory;
import org.apache.zeppelin.interpreter.InterpreterOption;
import org.apache.zeppelin.interpreter.InterpreterOutput;
import org.apache.zeppelin.interpreter.InterpreterSettingManager;
import org.apache.zeppelin.notebook.Notebook;
import org.apache.zeppelin.notebook.NotebookAuthorization;
import org.apache.zeppelin.notebook.repo.NotebookRepoSync;
import org.apache.zeppelin.rest.CredentialRestApi;
import org.apache.zeppelin.rest.HeliumRestApi;
import org.apache.zeppelin.rest.LoginRestApi;
import org.apache.zeppelin.rest.NotebookRepoRestApi;
import org.apache.zeppelin.rest.SecurityRestApi;
import org.apache.zeppelin.rest.ZeppelinRestApi;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.apache.zeppelin.search.LuceneSearch;
import org.apache.zeppelin.search.SearchService;
import org.apache.zeppelin.user.Credentials;
import org.apache.zeppelin.utils.SecurityUtils;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.server.SmartEngine;
import org.smartdata.server.rest.*;

import javax.servlet.DispatcherType;
import javax.ws.rs.core.Application;
import java.io.File;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

/**
 * Main class of embedded Zeppelin Server.
 */
public class SmartZeppelinServer {
  private static final Logger LOG = LoggerFactory.getLogger(SmartZeppelinServer.class);
  private static final String SMART_PATH_SPEC = "/smart/api/v1/*";
  private static final String ZEPPELIN_PATH_SPEC = "/api/*";

  private SmartEngine engine;
  private SmartConf conf;

  public static Notebook notebook;
  private ZeppelinConfiguration zconf;
  private Server jettyWebServer;
  private Helium helium;

  private InterpreterSettingManager interpreterSettingManager;
  private SchedulerFactory schedulerFactory;
  private InterpreterFactory replFactory;
  private SearchService noteSearchService;
  private NotebookRepoSync notebookRepo;
  private NotebookAuthorization notebookAuthorization;
  private Credentials credentials;
  private DependencyResolver depResolver;

  public SmartZeppelinServer(SmartConf conf, SmartEngine engine) throws Exception {
    this.conf = conf;
    this.engine = engine;

    this.zconf = ZeppelinConfiguration.create();

    // set     ZEPPELIN_ADDR and ZEPPELIN_PORT
    String httpAddr = conf.get(SmartConfKeys.SMART_SERVER_HTTP_ADDRESS_KEY,
        SmartConfKeys.SMART_SERVER_HTTP_ADDRESS_DEFAULT);
    String[] ipport = httpAddr.split(":");
    System.setProperty(ConfVars.ZEPPELIN_ADDR.getVarName(), ipport[0]);
    System.setProperty(ConfVars.ZEPPELIN_PORT.getVarName(), ipport[1]);

    // set zeppelin log dir
    String logDir = conf.get(SmartConfKeys.SMART_LOG_DIR_KEY, SmartConfKeys.SMART_LOG_DIR_DEFAULT);
    String zeppelinLogFile = logDir + "/zeppelin.log";
    System.setProperty("zeppelin.log.file", zeppelinLogFile);

    // set ZEPPELIN_CONF_DIR
    System.setProperty(ConfVars.ZEPPELIN_CONF_DIR.getVarName(),
        conf.get(SmartConfKeys.SMART_CONF_DIR_KEY, SmartConfKeys.SMART_CONF_DIR_DEFAULT));

    // set ZEPPELIN_HOME
    if (!isBinaryPackage(zconf)) {
      System.setProperty(ConfVars.ZEPPELIN_HOME.getVarName(), "smart-zeppelin/");
    }
  }


  private void init() throws Exception {
    this.depResolver = new DependencyResolver(
        zconf.getString(ConfVars.ZEPPELIN_INTERPRETER_LOCALREPO));

    InterpreterOutput.limit = zconf.getInt(ConfVars.ZEPPELIN_INTERPRETER_OUTPUT_LIMIT);

    HeliumApplicationFactory heliumApplicationFactory = new HeliumApplicationFactory();
    HeliumVisualizationFactory heliumVisualizationFactory;

    if (isBinaryPackage(zconf)) {
      /* In binary package, zeppelin-web/src/app/visualization and zeppelin-web/src/app/tabledata
       * are copied to lib/node_modules/zeppelin-vis, lib/node_modules/zeppelin-tabledata directory.
       * Check zeppelin/zeppelin-distribution/src/assemble/distribution.xml to see how they're
       * packaged into binary package.
       */
      heliumVisualizationFactory = new HeliumVisualizationFactory(
          zconf,
          new File(zconf.getRelativeDir(ConfVars.ZEPPELIN_DEP_LOCALREPO)),
          new File(zconf.getRelativeDir("lib/node_modules/zeppelin-tabledata")),
          new File(zconf.getRelativeDir("lib/node_modules/zeppelin-vis")));
    } else {
      heliumVisualizationFactory = new HeliumVisualizationFactory(
          zconf,
          new File(zconf.getRelativeDir(ConfVars.ZEPPELIN_DEP_LOCALREPO)),
          //new File(zconf.getRelativeDir("zeppelin-web/src/app/tabledata")),
          //new File(zconf.getRelativeDir("zeppelin-web/src/app/visualization")));
          new File(zconf.getRelativeDir("smart-zeppelin/zeppelin-web/src/app/tabledata")),
          new File(zconf.getRelativeDir("smart-zeppelin/zeppelin-web/src/app/visualization")));
    }

    this.helium = new Helium(
        zconf.getHeliumConfPath(),
        zconf.getHeliumDefaultLocalRegistryPath(),
        heliumVisualizationFactory,
        heliumApplicationFactory);

    // create visualization bundle
    try {
      heliumVisualizationFactory.bundle(helium.getVisualizationPackagesToBundle());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }

    this.schedulerFactory = new SchedulerFactory();
    this.interpreterSettingManager = new InterpreterSettingManager(zconf, depResolver,
        new InterpreterOption(true));
    this.notebookRepo = new NotebookRepoSync(zconf);
    this.noteSearchService = new LuceneSearch();
    this.notebookAuthorization = NotebookAuthorization.init(zconf);
    this.credentials = new Credentials(zconf.credentialsPersist(), zconf.getCredentialsPath());
  }

  private boolean isZeppelinWebEnabled() {
    return conf.getBoolean(SmartConfKeys.SMART_ENABLE_ZEPPELIN_WEB,
        SmartConfKeys.SMART_ENABLE_ZEPPELIN_WEB_DEFAULT);
  }

  public static void main(String[] args) throws Exception {
    SmartZeppelinServer server = new SmartZeppelinServer(new SmartConf(), null);

    server.start();

  }

  public void start() throws Exception {
    jettyWebServer = setupJettyServer(zconf);

    ContextHandlerCollection contexts = new ContextHandlerCollection();
    jettyWebServer.setHandler(contexts);

    // Web UI
    final WebAppContext webApp = setupWebAppContext(contexts);

    init();

    // REST api
    setupRestApiContextHandler(webApp);

    LOG.info("Starting zeppelin server");
    try {
      jettyWebServer.start(); //Instantiates ZeppelinServer
    } catch (Exception e) {
      LOG.error("Error while running jettyServer", e);
      //System.exit(-1);
    }
    LOG.info("Done, zeppelin server started");

    Runtime.getRuntime().addShutdownHook(new Thread(){
      @Override public void run() {
        LOG.info("Shutting down Zeppelin Server ... ");
        try {
          if (jettyWebServer != null) {
            jettyWebServer.stop();
          }
          if (notebook != null) {
            notebook.getInterpreterSettingManager().shutdown();
            notebook.close();
          }
          Thread.sleep(1000);
        } catch (Exception e) {
          LOG.error("Error while stopping servlet container", e);
        }
        LOG.info("Bye");
      }
    });
  }

  public void stop() {
    LOG.info("Shutting down Zeppelin Server ... ");
    try {
      if (jettyWebServer != null) {
        jettyWebServer.stop();
      }
      if (notebook != null) {
        notebook.getInterpreterSettingManager().shutdown();
        notebook.close();
      }
      Thread.sleep(1000);
    } catch (Exception e) {
      LOG.error("Error while stopping servlet container", e);
    }
    LOG.info("Bye");
  }

  private static Server setupJettyServer(ZeppelinConfiguration zconf) {

    final Server server = new Server();
    ServerConnector connector;

    if (zconf.useSsl()) {
      LOG.debug("Enabling SSL for Zeppelin Server on port " + zconf.getServerSslPort());
      HttpConfiguration httpConfig = new HttpConfiguration();
      httpConfig.setSecureScheme("https");
      httpConfig.setSecurePort(zconf.getServerSslPort());
      httpConfig.setOutputBufferSize(32768);
      httpConfig.setRequestHeaderSize(8192);
      httpConfig.setResponseHeaderSize(8192);
      httpConfig.setSendServerVersion(true);

      HttpConfiguration httpsConfig = new HttpConfiguration(httpConfig);
      SecureRequestCustomizer src = new SecureRequestCustomizer();
      // Only with Jetty 9.3.x
      // src.setStsMaxAge(2000);
      // src.setStsIncludeSubDomains(true);
      httpsConfig.addCustomizer(src);

      connector = new ServerConnector(
              server,
              new SslConnectionFactory(getSslContextFactory(zconf),
                  HttpVersion.HTTP_1_1.asString()),
              new HttpConnectionFactory(httpsConfig));
    } else {
      connector = new ServerConnector(server);
    }

    // Set some timeout options to make debugging easier.
    int timeout = 1000 * 30;
    connector.setIdleTimeout(timeout);
    connector.setSoLingerTime(-1);

    String webUrl = "";
    connector.setHost(zconf.getServerAddress());
    if (zconf.useSsl()) {
      connector.setPort(zconf.getServerSslPort());
      webUrl = "https://" + zconf.getServerAddress() + ":" + zconf.getServerSslPort();
    } else {
      connector.setPort(zconf.getServerPort());
      webUrl = "http://" + zconf.getServerAddress() + ":" + zconf.getServerPort();
    }

    LOG.info("Web address:" + webUrl);
    server.addConnector(connector);

    return server;
  }

  private static SslContextFactory getSslContextFactory(ZeppelinConfiguration zconf) {
    SslContextFactory sslContextFactory = new SslContextFactory();

    // Set keystore
    sslContextFactory.setKeyStorePath(zconf.getKeyStorePath());
    sslContextFactory.setKeyStoreType(zconf.getKeyStoreType());
    sslContextFactory.setKeyStorePassword(zconf.getKeyStorePassword());
    sslContextFactory.setKeyManagerPassword(zconf.getKeyManagerPassword());

    if (zconf.useClientAuth()) {
      sslContextFactory.setNeedClientAuth(zconf.useClientAuth());

      // Set truststore
      sslContextFactory.setTrustStorePath(zconf.getTrustStorePath());
      sslContextFactory.setTrustStoreType(zconf.getTrustStoreType());
      sslContextFactory.setTrustStorePassword(zconf.getTrustStorePassword());
    }

    return sslContextFactory;
  }

  class SmartRestApp extends Application {
    @Override
    public Set<Class<?>> getClasses() {
      Set<Class<?>> classes = new HashSet<>();
      return classes;
    }

    @Override
    public Set<Object> getSingletons() {
      Set<Object> singletons = new HashSet<>();

      SystemRestApi systemApi = new SystemRestApi(engine);
      singletons.add(systemApi);

      ConfRestApi confApi = new ConfRestApi(engine);
      singletons.add(confApi);

      ActionRestApi actionApi = new ActionRestApi(engine);
      singletons.add(actionApi);

      ClusterRestApi clusterApi = new ClusterRestApi(engine);
      singletons.add(clusterApi);

      CmdletRestApi cmdletApi = new CmdletRestApi(engine);
      singletons.add(cmdletApi);

      RuleRestApi ruleApi = new RuleRestApi(engine);
      singletons.add(ruleApi);

      NoteBookRestApi notebookApi = new NoteBookRestApi(engine);
      singletons.add(notebookApi);

      return singletons;
    }
  }

  class ZeppelinRestApp extends Application {
    @Override
    public Set<Class<?>> getClasses() {
      Set<Class<?>> classes = new HashSet<>();
      return classes;
    }

    @Override
    public Set<Object> getSingletons() {
      Set<Object> singletons = new HashSet<>();

      /** Rest-api root endpoint */
      ZeppelinRestApi root = new ZeppelinRestApi();
      singletons.add(root);
/*
      NotebookRestApi notebookApi =
        new NotebookRestApi(notebook, notebookWsServer, noteSearchService);
      singletons.add(notebookApi);*/

      NotebookRepoRestApi notebookRepoApi =
        new NotebookRepoRestApi(notebookRepo);
      singletons.add(notebookRepoApi);

      HeliumRestApi heliumApi = new HeliumRestApi(helium, notebook);
      singletons.add(heliumApi);

      CredentialRestApi credentialApi = new CredentialRestApi(credentials);
      singletons.add(credentialApi);

      SecurityRestApi securityApi = new SecurityRestApi();
      singletons.add(securityApi);

      LoginRestApi loginRestApi = new LoginRestApi();
      singletons.add(loginRestApi);

      return singletons;
    }
  }

  private void setupRestApiContextHandler(WebAppContext webApp) throws Exception {

    webApp.setSessionHandler(new SessionHandler());

    ResourceConfig smartConfig = new ApplicationAdapter(new SmartRestApp());
    ServletHolder smartServletHolder = new ServletHolder(new ServletContainer(smartConfig));
    webApp.addServlet(smartServletHolder, SMART_PATH_SPEC);

    ResourceConfig zeppelinConfig = new ApplicationAdapter(new ZeppelinRestApp());
    ServletHolder zeppelinServletHolder = new ServletHolder(new ServletContainer(zeppelinConfig));
    webApp.addServlet(zeppelinServletHolder, ZEPPELIN_PATH_SPEC);

    String shiroIniPath = zconf.getShiroPath();
    if (!StringUtils.isBlank(shiroIniPath)) {
      webApp.setInitParameter("shiroConfigLocations",
          new File(shiroIniPath).toURI().toString());
      SecurityUtils.initSecurityManager(shiroIniPath);
      webApp.addFilter(ShiroFilter.class, "/api/*", EnumSet.allOf(DispatcherType.class));
      webApp.addEventListener(new EnvironmentLoaderListener());
    }
  }

  private WebAppContext setupWebAppContext(ContextHandlerCollection contexts) {

    WebAppContext webApp = new WebAppContext();
    webApp.setContextPath(zconf.getServerContextPath());

    if (!isZeppelinWebEnabled()) {
      webApp.setResourceBase("");
      contexts.addHandler(webApp);
      return webApp;
    }

    File warPath = new File(zconf.getString(ConfVars.ZEPPELIN_WAR));
    //File warPath = new File("../dist/zeppelin-web-0.7.2.war");
    if (warPath.isDirectory()) {
      // Development mode, read from FS
      // webApp.setDescriptor(warPath+"/WEB-INF/web.xml");
      webApp.setResourceBase(warPath.getPath());
      webApp.setParentLoaderPriority(true);
    } else {
      // use packaged WAR
      webApp.setWar(warPath.getAbsolutePath());
      File warTempDirectory = new File(zconf.getRelativeDir(ConfVars.ZEPPELIN_WAR_TEMPDIR));
      warTempDirectory.mkdir();
      LOG.info("ZeppelinServer Webapp path: {}", warTempDirectory.getPath());
      webApp.setTempDirectory(warTempDirectory);
    }
    // Explicit bind to root
    webApp.addServlet(new ServletHolder(new DefaultServlet()), "/*");
    contexts.addHandler(webApp);

    webApp.addFilter(new FilterHolder(CorsFilter.class), "/*",
        EnumSet.allOf(DispatcherType.class));

    return webApp;
  }

  /**
   * Check if it is source build or binary package
   * @return
   */
  private static boolean isBinaryPackage(ZeppelinConfiguration conf) {
    return !new File(conf.getRelativeDir("smart-zeppelin/zeppelin-web")).isDirectory();
  }
}
