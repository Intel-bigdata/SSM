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

import java.util.concurrent.Semaphore;

/**
 * Controls the concurrency of actions execution.
 */
final public class ActionExecutor {
  // To control the concurrency of certain type of action
  static private Semaphore[] semaphores = new Semaphore[ActionType2.values().length];

  static {
    // TODO: make configurable
    semaphores[ActionType2.BalanceCluster.getValue()] = new Semaphore(1);
  }

  public static boolean run(ActionBase action) {
    int v = action.getActionType().getValue();
    boolean acquired = false;
    try {
      if (semaphores[v] != null) {
        semaphores[v].acquire();
      }
      acquired = true;
      return action.run();
    } catch (InterruptedException e) {
      e.printStackTrace();
      return false;
    } finally {
      if (acquired && semaphores[v] != null) {
        semaphores[v].release();
      }
    }
  }
}
