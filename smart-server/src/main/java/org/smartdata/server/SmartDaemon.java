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
import org.smartdata.SmartContext;
import org.smartdata.conf.SmartConf;
import org.smartdata.server.cluster.ClusterMembershipListener;
import org.smartdata.server.cluster.HazelcastInstanceProvider;
import org.smartdata.server.cluster.HazelcastWorker;
import org.smartdata.server.cluster.ServerDaemon;
import org.smartdata.server.utils.HazelcastUtil;

import java.io.IOException;

public class SmartDaemon implements ServerDaemon {
  private final String[] args;
  //Todo: maybe we can make worker as an interface
  private HazelcastWorker hazelcastWorker;

  public SmartDaemon(String[] args) {
    this.args = args;
  }

  public void start() throws IOException, InterruptedException {
    HazelcastInstance instance = HazelcastInstanceProvider.getInstance();
    if (HazelcastUtil.isMaster(instance)) {
      SmartServer.main(args);
    } else {
      instance.getCluster().addMembershipListener(new ClusterMembershipListener(this));
      this.hazelcastWorker = new HazelcastWorker(new SmartContext(new SmartConf()));
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

  public static void main(String[] args) {
    SmartDaemon daemon = new SmartDaemon(args);
    try {
      daemon.start();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
