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

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.http.HttpConfig.Policy;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class TestSSMHttpServer {
  private static final String BASEDIR = GenericTestUtils
          .getTempPath(TestSSMHttpServer.class.getSimpleName());
  private static String keystoresDir;
  private static String sslConfDir;
  private static Configuration conf;
  private static URLConnectionFactory connectionFactory;

  @Parameterized.Parameters
  public static Collection<Object[]> policy() {
    Object[][] params = new Object[][]{{Policy.HTTP_ONLY},
            {Policy.HTTPS_ONLY}, {Policy.HTTP_AND_HTTPS}};
    return Arrays.asList(params);
  }

  private final Policy policy;

  public TestSSMHttpServer(Policy policy) {
    super();
    this.policy = policy;
  }

  @BeforeClass
  public static void setUp() throws Exception {
    File base = new File(BASEDIR);
//    FileUtil.fullyDelete(base);
    base.mkdirs();
    conf = new Configuration();
//    keystoresDir = new File(BASEDIR).getAbsolutePath();
//    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestSSMHttpServer.class);
//    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
    connectionFactory = URLConnectionFactory
            .newDefaultURLConnectionFactory(conf);
//    conf.set(DFSConfigKeys.DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
//            KeyStoreTestUtil.getClientSSLConfigFileName());
//    conf.set(DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
//            KeyStoreTestUtil.getServerSSLConfigFileName());
  }


//  @BeforeClass
//  public static void setUp() throws Exception {
//    File base = new File(BASEDIR);
//    FileUtil.fullyDelete(base);
//    base.mkdirs();
//    conf = new Configuration();
//    keystoresDir = new File(BASEDIR).getAbsolutePath();
//   // sslConfDir = KeyStoreTestUtil.getClasspathDir(TestSSMHttpServer.class);
//    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
//    connectionFactory = URLConnectionFactory
//            .newDefaultURLConnectionFactory(conf);
//    conf.set(DFSConfigKeys.DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
//            KeyStoreTestUtil.getClientSSLConfigFileName());
//    conf.set(DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
//            KeyStoreTestUtil.getServerSSLConfigFileName());
//  }

  @AfterClass
  public static void tearDown() throws Exception {
    FileUtil.fullyDelete(new File(BASEDIR));
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }

  @Test
  public void testHttpPolicy() throws Exception {
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, Policy.HTTP_ONLY.name());
    conf.set(DFSConfigKeys.DFS_SSM_HTTPS_ADDRESS_KEY, "localhost:9494");
    InetSocketAddress addr = InetSocketAddress.createUnresolved("localhost", 9494);//port can't equal 0
    SSMHttpServer server = null;
    try {
      server = new SSMHttpServer(null, conf);
      server.start();
      Assert.assertTrue(implies(policy.isHttpEnabled(),
              canAccess("http", server.getHttpsAddress())));
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  private static boolean canAccess(String scheme, InetSocketAddress addr) {
    if (addr == null)
      return false;
    try {
      URL url = new URL(scheme + "://" + NetUtils.getHostPortString(addr));
      URLConnection conn = connectionFactory.openConnection(url);
      conn.connect();
      conn.getContent();
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  private static boolean implies(boolean a, boolean b) {
    return !a || b;
  }
}
