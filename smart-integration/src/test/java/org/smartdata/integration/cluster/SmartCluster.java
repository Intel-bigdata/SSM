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
package org.smartdata.integration.cluster;

import org.smartdata.conf.SmartConf;

/**
 * Interface for a Smart cluster.
 */
public interface SmartCluster {
  /**
   * Set up a cluster or attach to a specific cluster.
   */
  void setUp() throws Exception;

  /**
   * Get the configuration of the cluster.
   * @return
   */
  SmartConf getConf();

  /**
   * Cleaning-up work to quit the cluster.
   */
  void cleanUp() throws Exception;
}
