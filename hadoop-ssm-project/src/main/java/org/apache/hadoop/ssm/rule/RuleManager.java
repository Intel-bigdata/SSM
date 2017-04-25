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
package org.apache.hadoop.ssm.rule;

import java.io.IOException;

/**
 * Manage and execute rules.
 */
public class RuleManager {

  /**
   * Submit a rule to RuleManger.
   * @param rule
   * @param initState
   * @return
   * @throws IOException
   */
  public long submitRule(String rule, RuleState initState) throws IOException {
    return 0L;
  }

  /**
   * Init RuleManager, this includes:
   *    1. Load related data from local storage or HDFS
   *    2. Initial
   * @throws IOException
   */
  public void init() throws IOException {
  }

  /**
   * Start services
   */
  public void start() {
  }

  /**
   * Stop services
   */
  public void stop() {
  }

  /**
   * Waiting for threads to exit.
   */
  public void join() {
  }
}
