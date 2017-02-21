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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.AuthFilter;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.http.RestCsrfPreventionFilter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_ENABLED_KEY;

/**
 * Encapsulates the HTTP server started by the SSMHttpServer.
 */
@InterfaceAudience.Private
public class SSMHttpServer {
  private HttpServer2 httpServer;
  private final Configuration conf;
  private final InetSocketAddress bindAddress;

  SSMHttpServer(InetSocketAddress bindAddress, Configuration conf) {
    this.bindAddress = bindAddress;
    this.conf = conf;
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

    final String pathSpec = WebHdfsFileSystem.PATH_PREFIX + "/*";
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
//    httpServer.addJerseyResourcePackage(SSMWebMethods.class
//                    .getPackage().getName() + ";" + Param.class.getPackage().getName(),
//            pathSpec);
  }

  void start() throws IOException, URISyntaxException {
    HttpServer2.Builder builder = new HttpServer2.Builder().setName("hdfs")
            .setConf(conf);//Name need modify later

//    .setACL(new AccessControlList(conf.get(DFS_ADMIN, " "))
//    builder.setSecurityEnabled(true);
    if (bindAddress.getPort() == 0) {
      builder.setFindPort(true);
    }
    URI uri = URI.create("http://" + NetUtils.getHostPortString(bindAddress));
    builder.addEndpoint(uri);
    httpServer = builder.build();
    init(conf);
//    setupServlets(httpServer, conf);
    httpServer.start();
  }

  void stop() throws Exception {
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
            DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY));
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
}



//    final boolean xFrameEnabled = conf.getBoolean(
//            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
//            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);
//    final String xFrameOptionValue = conf.getTrimmed(
//            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
//            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);
//    builder.configureXFrame(xFrameEnabled).setXFrameOption(xFrameOptionValue);

//    builder.setName("hdfs");
//    URI ssmServerUri = new URI("http://127.0.0.1:0");//127.0.0.1
//    builder.addEndpoint(ssmServerUri);