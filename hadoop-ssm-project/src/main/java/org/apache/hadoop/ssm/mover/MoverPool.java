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
package org.apache.hadoop.ssm.mover;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MoverPool : A singleton class to manage all Mover actions.
 */
public class MoverPool {
  private static MoverPool instance = new MoverPool();
  private Configuration conf = new HdfsConfiguration();

  public static MoverPool getInstance() {
    return instance;
  }

  private Map<UUID, Status> moverMap;

  private MoverPool() {
    moverMap = new ConcurrentHashMap<>();
  }

  /**
   * Initialize the HDFS configuration of MoverPool at the beginning of
   * the SSM service.
   * @param configuration
   */
  public void init(Configuration configuration) {
    conf = configuration;
  }

  /**
   * Create a Mover event.
   * @param dir the directory to enforce the storage policy using Mover tool.
   * @return the UUID of this action for user to track status
   */
  public UUID createMoverAction(String dir) {
    UUID id = UUID.randomUUID();
    Status status = new MoverStatus();
    moverMap.put(id, status);
    Thread moverThread = new MoverProcess(status, dir);
    moverThread.start();
    return id;
  }

  class MoverProcess extends Thread {
    private String path;
    private Mover.Cli moverClient;

    public MoverProcess(Status status, String path) {
      this.moverClient = new Mover.Cli(status);
      this.path = path;
    }

    @Override
    public void run() {
      try {
        int result = ToolRunner.run(conf, moverClient,
            new String[] {"-p", path});
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Get the status of a certain moving event.
   * @param id the UUID of the event
   * @return the status of the event
   */
  public Status getStatus(UUID id) {
    return moverMap.get(id);
  }

  /**
   * Remove the status of a certain moving event.
   * After removing, the status of this event can no longer be tracked.
   * User should remove the status when the event is finished and the status of
   * it is no longer needed.
   * @param id the UUID of the event
   */
  public void removeStatus(UUID id) {
    moverMap.remove(id);
  }

  public void stop(UUID id) {
    // TODO: stop the Mover action
  }

  public void halt(UUID id) {
    // TODO: halt the Mover action
  }

  public void restart(UUID id) {
    // TODO: restart the Mover action
  }
}
