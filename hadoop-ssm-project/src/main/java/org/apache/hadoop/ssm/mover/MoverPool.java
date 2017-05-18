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

  synchronized public static MoverPool getInstance() {
    return instance;
  }

  private Map<UUID, Status> moverMap;
  private Map<UUID, Thread> moverThreads;

  private MoverPool() {
    moverMap = new ConcurrentHashMap<>();
    moverThreads = new ConcurrentHashMap<>();
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
   * @param path the directory to enforce the storage policy using Mover tool.
   * @return the UUID of this action for user to track status
   */
  public UUID createMoverAction(String path) {
    UUID id = UUID.randomUUID();
    Status status = new MoverStatus();
    moverMap.put(id, status);
    Thread moverThread = new MoverProcess(status, path);
    moverThreads.put(id, moverThread);
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

    public String getPath() {
      return path;
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
    moverThreads.remove(id);
  }

  /**
   * Stop a moving event.
   * @param id the id of the event
   * @param retryTimes the total retry times for the stop operation, each retry
   * waits for 500 ms
   * @return true if stopped or false if cannot be stopped after all retries
   */
  public Boolean stop(UUID id, int retryTimes) throws Exception {
    Thread moverThread = moverThreads.get(id);
    if (moverThread == null) {
      return false;
    }
    if (moverThread.isAlive()) {
      for (int index = 1; index < retryTimes; index ++) {
        moverThread.interrupt();
        Thread.sleep(500);
        if (getStatus(id).getIsFinished()) {
          return true;
        }
      }
    }
    return false;
  }

  public void halt(UUID id) {
    // TODO: halt the Mover action
  }

  /**
   * Restart a moving event.
   * @param id the id of the event
   * @return true if stop or false if the id cannot be found
   */
  public Boolean restart(UUID id) throws Exception{
    Thread moverThread = moverThreads.get(id);
    if (moverThread == null) {
      return false;
    }

    if (moverThread.isAlive()) {
      moverThread.interrupt();
      while (!getStatus(id).getIsFinished()) {
        Thread.sleep(300);
      }
    }

    getStatus(id).reset();
    String path = null;
    if (moverThread instanceof MoverProcess) {
      path = ((MoverProcess) moverThread).getPath();
    }
    moverThread = new MoverProcess(getStatus(id), path);
    moverThreads.remove(id);
    moverThreads.put(id, moverThread);
    moverThread.start();
    return true;
  }
}
