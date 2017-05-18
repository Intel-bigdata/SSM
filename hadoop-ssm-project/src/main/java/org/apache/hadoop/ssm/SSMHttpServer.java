/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ssm;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_ENABLED_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.servlet.ServletContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FsckServlet;
import org.apache.hadoop.hdfs.server.namenode.ImageServlet;
import org.apache.hadoop.hdfs.server.namenode.StartupProgressServlet;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.web.AuthFilter;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.http.RestCsrfPreventionFilter;

import com.google.common.annotations.VisibleForTesting;

/**
 * Encapsulates the HTTP server started by the SSMHttpServer.
 */
public class SSMHttpServer {
  private SSMServer ssm;

  private HttpServer2 httpServer;
  private final Configuration conf;

  private InetSocketAddress httpAddress;
  private InetSocketAddress httpsAddress;
  private final InetSocketAddress bindAddress;

  public static final String SSM_ADDRESS_ATTRIBUTE_KEY = "ssm.address";
  public static final String FSIMAGE_ATTRIBUTE_KEY = "name.system.image";
  public static final String STARTUP_PROGRESS_ATTRIBUTE_KEY = "startup.progress";

  public SSMHttpServer(SSMServer ssm, Configuration conf) {
    this.ssm = ssm;
    this.conf = conf;
    this.bindAddress = getHttpServerAddress();
  }

  private InetSocketAddress getHttpServerAddress() {
    String[] strings = conf.get(SSMConfigureKeys.DFS_SSM_HTTP_ADDRESS_KEY,
        SSMConfigureKeys.DFS_SSM_HTTP_ADDRESS_DEFAULT).split(":");
    return new InetSocketAddress(strings[strings.length - 2]
        , Integer.parseInt(strings[strings.length - 1]));
  }

  private void init(Configuration conf) throws IOException {
    // set user pattern based on configuration file
    UserParam.setUserPattern(conf.get(
            HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_KEY,
            HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_DEFAULT));
    // add authentication filter for webhdfs
    final String className = conf.get(
            DFSConfigKeys.DFS_WEBHDFS_AUTHENTICATION_FILTER_KEY,
            DFSConfigKeys.DFS_WEBHDFS_AUTHENTICATION_FILTER_DEFAULT);
    final String name = className;

//    final String pathSpec = WebHdfsFileSystem.PATH_PREFIX + "/*";
    final String pathSpec = "/ssm/v1/*";
    Map<String, String> params = getAuthFilterParams(conf);
    HttpServer2.defineFilter(httpServer.getWebAppContext(), name, className,
            params, new String[]{pathSpec});
    HttpServer2.LOG.info("Added filter '" + name + "' (class=" + className
            + ")");

    // add REST CSRF prevention filter
    if (conf.getBoolean(DFS_WEBHDFS_REST_CSRF_ENABLED_KEY,
            DFS_WEBHDFS_REST_CSRF_ENABLED_DEFAULT)) {
      Map<String, String> restCsrfParams = RestCsrfPreventionFilter
              .getFilterParams(conf, "dfs.webhdfs.rest-csrf.");
      String restCsrfClassName = RestCsrfPreventionFilter.class.getName();
      HttpServer2.defineFilter(httpServer.getWebAppContext(), restCsrfClassName,
              restCsrfClassName, restCsrfParams, new String[]{pathSpec});
    }
    httpServer.addJerseyResourcePackage(SSMWebMethods.class
                    .getPackage().getName() + ";" + Param.class.getPackage().getName(),
            pathSpec);
  }

  public void start() throws IOException, URISyntaxException {
    HttpConfig.Policy policy = DFSUtil.getHttpPolicy(conf);

    final InetSocketAddress httpAddr = bindAddress;
    final String httpsAddrString = conf.getTrimmed(
            DFSConfigKeys.DFS_SSM_HTTPS_ADDRESS_KEY,
            DFSConfigKeys.DFS_SSM_HTTPS_ADDRESS_DEFAULT);
    InetSocketAddress httpsAddr = NetUtils.createSocketAddr(httpsAddrString);

    HttpServer2.Builder builder = DFSUtil.httpServerTemplateForSSM(conf,
            httpAddr, httpsAddr, "command",
            DFSConfigKeys.DFS_SSM_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
            DFSConfigKeys.DFS_SSM_KEYTAB_FILE_KEY);
    builder.setFindPort(true);
    final boolean xFrameEnabled = conf.getBoolean(
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

    final String xFrameOptionValue = conf.getTrimmed(
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

    builder.configureXFrame(xFrameEnabled).setXFrameOption(xFrameOptionValue);
    httpServer = builder.build();
//    if (policy.isHttpsEnabled()) {
//      // assume same ssl port for ssm and the defult port is 9965
//      InetSocketAddress ssmSslPort = NetUtils.createSocketAddr(conf.getTrimmed(
//              DFSConfigKeys.DFS_SSM_HTTPS_ADDRESS_KEY, infoHost + ":"
//                      + DFSConfigKeys.DFS_SSM_HTTPS_DEFAULT_PORT));
//      httpServer.setAttribute(DFSConfigKeys.DFS_SSM_HTTPS_PORT_KEY,
//              ssmSslPort.getPort());
//    }

    init(conf);
    httpServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    setupServlets(httpServer, conf);
    httpServer.start();

    int connIdx = 0;
    if (policy.isHttpEnabled()) {
      httpAddress = httpServer.getConnectorAddress(connIdx++);
      conf.set(DFSConfigKeys.DFS_SSM_HTTP_ADDRESS_KEY,
              NetUtils.getHostPortString(httpAddress));
    }

    if (policy.isHttpsEnabled()) {
      httpsAddress = httpServer.getConnectorAddress(connIdx);
      conf.set(DFSConfigKeys.DFS_SSM_HTTPS_ADDRESS_KEY,
              NetUtils.getHostPortString(httpsAddress));
    }
  }


  public void stop() throws Exception {
    if (httpServer != null) {
      httpServer.stop();
    }
  }

  /**
   * Joins the httpserver.
   */
  public void join() throws InterruptedException {
    if (httpServer != null) {
      httpServer.join();
    }
  }

  private static void setupServlets(HttpServer2 httpServer, Configuration conf) {
    httpServer.addInternalServlet("startupProgress",
            StartupProgressServlet.PATH_SPEC, StartupProgressServlet.class);
    httpServer.addInternalServlet("fsck", "/fsck", FsckServlet.class,
            true);
    httpServer.addInternalServlet("imagetransfer", ImageServlet.PATH_SPEC,
            ImageServlet.class, true);
  }

  private Map<String, String> getAuthFilterParams(Configuration conf)
          throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    // Select configs beginning with 'dfs.web.authentication.'
    Iterator<Map.Entry<String, String>> iterator = conf.iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, String> kvPair = iterator.next();
      if (kvPair.getKey().startsWith(AuthFilter.CONF_PREFIX)) {
        params.put(kvPair.getKey(), kvPair.getValue());
      }
    }
    String principalInConf = conf
            .get(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY);
    if (principalInConf != null && !principalInConf.isEmpty()) {
      params
              .put(
                      DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY,
                      SecurityUtil.getServerPrincipal(principalInConf,
                              bindAddress.getHostName()));
    }
    String httpKeytab = conf.get(DFSUtil.getSpnegoKeytabKey(conf,
            DFSConfigKeys.DFS_SSM_KEYTAB_FILE_KEY));
    if (httpKeytab != null && !httpKeytab.isEmpty()) {
      params.put(
              DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY,
              httpKeytab);
    }
    String anonymousAllowed = conf
            .get(DFSConfigKeys.DFS_WEB_AUTHENTICATION_SIMPLE_ANONYMOUS_ALLOWED);
    if (anonymousAllowed != null && !anonymousAllowed.isEmpty()) {
      params.put(
              DFSConfigKeys.DFS_WEB_AUTHENTICATION_SIMPLE_ANONYMOUS_ALLOWED,
              anonymousAllowed);
    }
    return params;
  }

  /**
   * Get SPNEGO keytab Key from configuration
   *
   * @param conf       Configuration
   * @param defaultKey default key to be used for config lookup
   * @return DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY if the key is not empty
   * else return defaultKey
   */
  public static String getSpnegoKeytabKey(Configuration conf, String defaultKey) {
    String value =
            conf.get(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY);
    return (value == null || value.isEmpty()) ?
            defaultKey : DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY;
  }

  InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  InetSocketAddress getHttpsAddress() {
    return httpsAddress;
  }

  /**
   * Sets fsimage for use by servlets.
   *
   * @param fsImage FSImage to set
   */
  void setFSImage(FSImage fsImage) {
    httpServer.setAttribute(FSIMAGE_ATTRIBUTE_KEY, fsImage);
  }

  /**
   * Sets address of namenode for use by servlets.
   *
   * @param ssmAddress InetSocketAddress to set
   */
  void setNameNodeAddress(InetSocketAddress ssmAddress) {
    httpServer.setAttribute(SSM_ADDRESS_ATTRIBUTE_KEY,
            NetUtils.getConnectAddress(ssmAddress));
  }

  /**
   * Sets startup progress of namenode for use by servlets.
   *
   * @param prog StartupProgress to set
   */
  void setStartupProgress(StartupProgress prog) {
    httpServer.setAttribute(STARTUP_PROGRESS_ATTRIBUTE_KEY, prog);
  }

  static FSImage getFsImageFromContext(ServletContext context) {
    return (FSImage) context.getAttribute(FSIMAGE_ATTRIBUTE_KEY);
  }

  static Configuration getConfFromContext(ServletContext context) {
    return (Configuration) context.getAttribute(JspHelper.CURRENT_CONF);
  }

  public static InetSocketAddress getSSMAddressFromContext(
          ServletContext context) {
    return (InetSocketAddress) context.getAttribute(
            SSM_ADDRESS_ATTRIBUTE_KEY);
  }

  /**
   * Returns StartupProgress associated with ServletContext.
   *
   * @param context ServletContext to get
   * @return StartupProgress associated with context
   */
  static StartupProgress getStartupProgressFromContext(
          ServletContext context) {
    return (StartupProgress) context.getAttribute(STARTUP_PROGRESS_ATTRIBUTE_KEY);
  }

  /**
   * Returns the httpServer.
   *
   * @return HttpServer2
   */
  @VisibleForTesting
  public HttpServer2 getHttpServer() {
    return httpServer;
  }
}
