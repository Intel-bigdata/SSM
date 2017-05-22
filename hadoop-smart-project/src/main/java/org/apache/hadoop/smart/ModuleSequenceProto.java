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
package org.apache.hadoop.smart;

import org.apache.hadoop.smart.sql.DBAdapter;

import java.io.IOException;

public interface ModuleSequenceProto {
  enum State {
    INITING,
    INITED,
    STARTING,
    STARTED,
    STOPPING,
    STOPPED,
    JOINING,
    JOINED
  }

  /**
   * Init module using info from configuration and database.
   * @param dbAdapter
   * @return
   * @throws IOException
   */
  boolean init(DBAdapter dbAdapter) throws IOException;

  /**
   * After start call, all services and public calls should work.
   * @return
   * @throws IOException
   */
  boolean start() throws IOException, InterruptedException;

  /**
   * After stop call, all states in database will not be changed anymore.
   * @throws IOException
   */
  void stop() throws IOException;

  /**
   * Stop threads and other cleaning jobs.
   * @throws IOException
   */
  void join() throws IOException;
}
