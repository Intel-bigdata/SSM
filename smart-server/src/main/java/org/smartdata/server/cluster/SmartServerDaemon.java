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
package org.smartdata.server.cluster;

import com.hazelcast.core.HazelcastInstance;
import org.smartdata.SmartContext;
import org.smartdata.conf.SmartConf;
import org.smartdata.server.cmdlet.CmdletFactory;
import org.smartdata.server.cmdlet.CmdletManager;
import org.smartdata.server.cmdlet.hazelcast.HazelcastWorker;
import org.smartdata.server.utils.HazelcastUtil;

import java.io.IOException;

public class SmartServerDaemon {
  private final String[] args;
  //Todo: maybe we can make worker as an interface
  private HazelcastWorker hazelcastWorker;

  public SmartServerDaemon(String[] args) {
    this.args = args;
  }

  public void start() throws IOException, InterruptedException {
    HazelcastInstance instance = HazelcastInstanceProvider.getInstance();
    if (HazelcastUtil.isMaster(instance)) {
      CmdletManager manager = new CmdletManager();
      Thread.sleep(10000);
      manager.start();
      //SmartServer.main(args);
    } else {
      instance.getCluster().addMembershipListener(new ClusterMembershipListener(this));
      CmdletFactory factory = new CmdletFactory(new SmartContext(new SmartConf()));
      this.hazelcastWorker = new HazelcastWorker(factory);
      this.hazelcastWorker.start();
    }
  }

  public void becomeActive() {
    if (this.hazelcastWorker != null) {
      this.hazelcastWorker.stop();
    }
    //SmartServer.main(args);
  }

  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    SmartServerDaemon daemon = new SmartServerDaemon(args);
    daemon.start();
  }
}