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
import org.smartdata.hdfs.action.SchedulePlan;

import java.io.IOException;
import java.net.URI;

/**
 * HDFS move based move runner.
 */
public class MoverBasedMoveRunner extends MoveRunner {
  private static final Logger LOG = LoggerFactory.getLogger(MoverBasedMoveRunner.class);
  private Configuration conf;
  private MoverStatus actionStatus;

  static class Pair {
    URI ns;
    Path path;

    Pair(URI ns, Path path) {
      this.ns = ns;
      this.path = path;
    }
  }

  public MoverBasedMoveRunner(Configuration conf, MoverStatus actionStatus) {
    this.conf = conf;
    this.actionStatus = actionStatus;
  }

  @Override
  public void move(String file, SchedulePlan plan) throws Exception {
    if (plan == null) {
      move(file);
    } else {
      MoverExecutor executor = new MoverExecutor(conf, 10, 20);
      executor.executeMove(plan);
    }
  }

  @Override
  public void move(String file) throws Exception {
    throw new IOException("Mover plan not specified.");
  }
}
