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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFS move based move runner.
 */
public class OldMoverBasedMoveRunner extends OldMoveRunner {
    private static final Logger LOG = LoggerFactory.getLogger(
            MoverBasedMoveRunner.class);
    private Configuration conf;
    private MoverStatus actionStatus;

    public OldMoverBasedMoveRunner(Configuration conf, MoverStatus actionStatus) {
        this.conf = conf;
        this.actionStatus = actionStatus;
    }

    @Override
    public void move(String file) throws Exception {
        this.move(new String[] {file});
    }

    @Override
    public void move(String[] files) throws Exception {
        OldMoverCli moverCli = new OldMoverCli(actionStatus);
        ToolRunner.run(conf, moverCli, files);
    }
}

