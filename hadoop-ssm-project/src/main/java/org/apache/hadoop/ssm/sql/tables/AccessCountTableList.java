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
package org.apache.hadoop.ssm.sql.tables;

import java.util.ArrayList;
import java.util.List;

public class AccessCountTableList {
  private List<AccessCountTable> tables;
  private TableAddOpListener listener;

  public AccessCountTableList() {
    this(null);
  }

  public AccessCountTableList(TableAddOpListener listener) {
    this.tables = new ArrayList<>();
    this.listener = listener;
  }

  public void addTable(AccessCountTable table) {
    this.tables.add(table);
    if (this.listener != null) {
      this.listener.tableAdded(this, table);
    }
  }

  public List<AccessCountTable> getTables(Long start, Long end) {
    List<AccessCountTable> results = new ArrayList<>();
    for (AccessCountTable table : tables) {
      if (table.getStartTime() >= start && table.getEndTime() <= end) {
        results.add(table);
      }
    }
    return results;
  }

  public boolean contains(AccessCountTable table) {
    return this.tables.contains(table);
  }
}
