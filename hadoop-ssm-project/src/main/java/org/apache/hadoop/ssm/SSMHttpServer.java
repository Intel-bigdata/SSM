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
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.AccessControlList;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ADMIN;

/**
 * Encapsulates the HTTP server started by the NameNode.
 */
@InterfaceAudience.Private
public class SSMHttpServer {
  private static HttpServer2 httpServer;
  private final Configuration conf;
//  private InetSocketAddress httpAddress;
//  private InetSocketAddress httpsAddress;
  private final InetSocketAddress bindAddress;

  SSMHttpServer(InetSocketAddress bindAddress, Configuration conf) {
    this.bindAddress = bindAddress;
    this.conf = conf;
  }

  private static void init() {
    final String pathSpec = WebHdfsFileSystem.PATH_PREFIX + "/*";
//    httpServer.addJerseyResourcePackage(SSMWebMethods.class
//                    .getPackage().getName() + ";" + Param.class.getPackage().getName(),
//            pathSpec);
  }
  
  void start() throws IOException, URISyntaxException {
    HttpServer2.Builder builder = new HttpServer2.Builder().setName("hdfs")
            .setConf(conf).setACL(new AccessControlList(conf.get(DFS_ADMIN, " ")));
    builder.setSecurityEnabled(true);
    if (bindAddress.getPort() == 0) {
      builder.setFindPort(true);
    }
    URI uri = URI.create("http://" + NetUtils.getHostPortString(bindAddress));
    builder.addEndpoint(uri);

    httpServer = builder.build();
//    setupServlets(httpServer, conf);
    httpServer.start();
  }

  void stop() throws Exception {
    if (httpServer != null) {
      httpServer.stop();
    }
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