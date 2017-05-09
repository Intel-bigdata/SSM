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
package org.apache.hadoop.ssm;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ssm.sql.DBAdapter;

import java.io.IOException;

/**
 * Polls metrics and events from NameNode
 */
public class StatesManager implements ModuleSequenceProto {
  private SSMServer ssm;
  private Configuration conf;

  public StatesManager(SSMServer ssm, Configuration conf) {
    this.ssm = ssm;
    this.conf = conf;
  }

  /**
   * Load configure/data to initialize.
   * @return true if initialized successfully
   */
  public boolean init(DBAdapter dbAdapter) throws IOException {
    return true;
  }

  /**
   * Start daemon threads in StatesManager for function.
   */
  public boolean start() throws IOException {
    return true;
  }

  public void stop() throws IOException {
  }

  public void join() throws IOException {
  }

  /**
   * RuleManger uses this function to subscribe events interested.
   * StatesManager poll these events from NN or generate these events.
   * That is, for example, if no rule interests in FileOpen event then
   * StatesManager will not get these info from NN.
   */
  public void subscribeEvent() {
  }

  /**
   * After unsubscribe the envent, it will not be notified when the
   * event happened.
   */
  public void unsubscribeEvent() {
  }
}
