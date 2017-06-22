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
package org.smartdata.server.engine.cmdlet;

import org.smartdata.common.CmdletState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.server.engine.CmdletExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * CmdletPool : A singleton class to manage all cmdletsThread
 *
 * @deprecated Will be removed after using new executor service framework
 */
@Deprecated
public class CmdletPool {
  static final Logger LOG = LoggerFactory.getLogger(CmdletExecutor.class);

  private Map<Long, Cmdlet> cmdletMap;
  private Map<Long, Thread> cmdletThread;

  public CmdletPool() {
    cmdletMap = new ConcurrentHashMap<>();
    cmdletThread = new ConcurrentHashMap<>();
  }

  public int size() {
    return cmdletMap.size();
  }

  public void stop() throws Exception {
    for (Long cid : cmdletMap.keySet()) {
      deleteCmdlet(cid);
    }
  }

  public void setFinished(long cid, CmdletState status) {
    getCmdlet(cid).setState(status);
  }

  /**
   * Delete a cmdlet from the pool
   *
   * @param cid
   * @throws IOException
   */
  public void deleteCmdlet(long cid) throws IOException {
    if (!cmdletMap.containsKey(cid)) {
      return;
    }
    Cmdlet cmd = cmdletMap.get(cid);
    if (cmd.getState() != CmdletState.DONE) {
      LOG.info("Force Terminate Cmdlet {}", cmd.toString());
      cmd.stop();
    }
    cmdletMap.remove(cid);
    cmdletThread.remove(cid);
  }

  public Cmdlet getCmdlet(long cid) {
    return cmdletMap.get(cid);
  }

  public List<Cmdlet> getcmdlets() {
    return new ArrayList<Cmdlet>(cmdletMap.values());
  }

  public Thread getCmdletThread(long cid) {
    return cmdletThread.get(cid);
  }

  public void execute(Cmdlet cmd) {
    cmdletMap.put(cmd.getId(), cmd);
    Thread cthread = new Thread(cmd);
    cmdletThread.put(cmd.getId(), cthread);
    cthread.start();
  }
}
