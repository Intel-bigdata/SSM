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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A light-weight executor for Mover.
 */
public class MoverExecutor {
  static final Logger LOG = LoggerFactory.getLogger(MoverExecutor.class);

  private Dispatcher dispatcher;
  private Dispatcher.Source source;
  private Dispatcher.StorageGroup target;
  private Dispatcher.DBlock db;

  public MoverExecutor(Dispatcher dispatcher, Dispatcher.DBlock db,
      Dispatcher.Source source, Dispatcher.StorageGroup target) {
    this.dispatcher = dispatcher;
    this.source = source;
    this.target = target;
    this.db = db;
  }

  // Execute a move action providing source and target
  // TODO: temporarily make use of Dispatcher, may need refactor
  public boolean executeMove() {
    final Dispatcher.PendingMove pm = source.addPendingMove(db, target);
    if (pm != null) {
      dispatcher.executePendingMove(pm);
      return true;
    }
    return false;
  }
}
