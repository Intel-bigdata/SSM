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

import com.hazelcast.core.HazelcastInstance;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.server.cluster.ClusterMembershipListener;
import org.smartdata.server.cluster.HazelcastInstanceProvider;
import org.smartdata.server.cluster.HazelcastWorker;
import org.smartdata.server.cluster.ServerDaemon;
import org.smartdata.server.utils.HazelcastUtil;
import org.smartdata.utils.SecurityUtil;

import java.io.IOException;

public class SmartDaemon implements ServerDaemon {
  private static final Logger LOG = LoggerFactory.getLogger(SmartDaemon.class);
  private final String[] args;
  //Todo: maybe we can make worker as an interface
  private HazelcastWorker hazelcastWorker;

  public SmartDaemon(String[] args) {
    this.args = args;
  }

  public void start() throws IOException, InterruptedException {
    SmartConf conf = new SmartConf();
    authentication(conf);
    HazelcastInstance instance = HazelcastInstanceProvider.getInstance();
    if (HazelcastUtil.isMaster(instance)) {
      SmartServer.main(args);
    } else {
      HadoopUtil.setSmartConfByHadoop(conf);

      String rpcHost = HazelcastUtil
              .getMasterMember(HazelcastInstanceProvider.getInstance())
              .getAddress()
              .getHost();
      String rpcPort = conf
              .get(SmartConfKeys.SMART_SERVER_RPC_ADDRESS_KEY,
                      SmartConfKeys.SMART_SERVER_RPC_ADDRESS_DEFAULT)
              .split(":")[1];
      conf.set(SmartConfKeys.SMART_SERVER_RPC_ADDRESS_KEY, rpcHost + ":" + rpcPort);

      instance.getCluster().addMembershipListener(new ClusterMembershipListener(this, conf));
      this.hazelcastWorker = new HazelcastWorker(new SmartContext(conf));
      this.hazelcastWorker.start();
    }
  }

  @Override
  public void becomeActive() {
    if (this.hazelcastWorker != null) {
      this.hazelcastWorker.stop();
    }
    SmartServer.main(args);
  }

  private void authentication(SmartConf conf) throws IOException {
    if (!SecurityUtil.isSecurityEnabled(conf)) {
      return;
    }

    // Load Hadoop configuration files
    try {
      HadoopUtil.loadHadoopConf(conf);
    } catch (IOException e) {
      LOG.info("Running in secure mode, but cannot find Hadoop configuration file. "
              + "Please config smart.hadoop.conf.path property in smart-site.xml.");
      conf.set("hadoop.security.authentication", "kerberos");
      conf.set("hadoop.security.authorization", "true");
    }

    UserGroupInformation.setConfiguration(conf);

    String keytabFilename = conf.get(SmartConfKeys.SMART_SERVER_KEYTAB_FILE_KEY);
    String principalConfig = conf.get(SmartConfKeys.SMART_SERVER_KERBEROS_PRINCIPAL_KEY);
    String principal =
       org.apache.hadoop.security.SecurityUtil.getServerPrincipal(principalConfig, (String) null);

    SecurityUtil.loginUsingKeytab(keytabFilename, principal);
  }

  public static void main(String[] args) {
    SmartDaemon daemon = new SmartDaemon(args);
    try {
      daemon.start();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
