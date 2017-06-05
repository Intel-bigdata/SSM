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
package org.smartdata.actions.hdfs.move;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.smartdata.actions.ActionStatus;

import java.io.IOException;
import java.io.PrintStream;

/**
 * HDFS move based move runner.
 */
public class MoverBasedMoveRunner extends MoveRunner {
  private Configuration conf;
  private ActionStatus actionStatus;
  private PrintStream log;

  public MoverBasedMoveRunner(Configuration conf, ActionStatus actionStatus) {
    this.conf = conf;
    this.actionStatus = actionStatus;
    log = actionStatus.getLogPrintStream();
  }

  @Override
  public void move(String file) throws IOException {
    Thread moverProcess = new MoverProcess(actionStatus, new String[] {file});
    moverProcess.start();
  }

  @Override
  public void move(String[] files) throws IOException {
    Thread moverProcess = new MoverProcess(actionStatus, files);
    moverProcess.start();
  }

  class MoverProcess extends Thread {
    private String[] paths;
    private MoverCli moverClient;
    private long id;

    public MoverProcess(ActionStatus status, String[] paths) {
      this.moverClient = new MoverCli(status);
      id = status.getId();
      this.paths = paths;
    }

    @Override
    public void run() {
      try {
        log.println("Start move : id = " + id);
        int result = ToolRunner.run(conf, moverClient, paths);
        log.println("Finish move : id = " + id);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
