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
package org.smartdata.hdfs.action.move;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.model.action.FileMovePlan;

import java.io.PrintStream;
import java.net.URI;

/**
 * HDFS move based move runner.
 */
public class MoverBasedMoveRunner {
  private static final Logger LOG = LoggerFactory.getLogger(MoverBasedMoveRunner.class);
  private Configuration conf;
  private MoverStatus actionStatus;
  private PrintStream resultOs;
  private PrintStream logOs;

  static class Pair {
    URI ns;
    Path path;

    Pair(URI ns, Path path) {
      this.ns = ns;
      this.path = path;
    }
  }

  public MoverBasedMoveRunner(Configuration conf, MoverStatus actionStatus,
      PrintStream resultOs, PrintStream logOs) {
    this.conf = conf;
    this.actionStatus = actionStatus;
    this.resultOs = resultOs;
    this.logOs = logOs;
  }

  public int move(String file, FileMovePlan plan) throws Exception {
    int maxMoves = plan.getPropertyValueInt(FileMovePlan.MAX_CONCURRENT_MOVES, 10);
    int maxRetries = plan.getPropertyValueInt(FileMovePlan.MAX_NUM_RETRIES, 10);
    MoverExecutor executor = new MoverExecutor(actionStatus, conf, maxRetries, maxMoves);
    return executor.executeMove(plan, resultOs, logOs);
  }
}
