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
package org.smartdata.server.metastore.tables;

import org.smartdata.server.metastore.MetaStore;

public abstract class TableEvictor {
  private MetaStore adapter;

  public TableEvictor(MetaStore adapter) {
    this.adapter = adapter;
  }

  public void dropTable(AccessCountTable accessCountTable) {
//    try {
//      this.adapter.dropTable(accessCountTable.getTableName());
//    } catch (SQLException e) {
//      e.printStackTrace();
//    }
  }

  abstract void evictTables(AccessCountTableDeque tables, int size);
}
