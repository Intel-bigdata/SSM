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
package org.smartdata.metastore.dao;

import org.smartdata.metastore.MetaStore;

import java.util.Iterator;

public class DurationEvictor extends TableEvictor {
  private final long duration;

  public DurationEvictor(MetaStore adapter, long duration) {
    super(adapter);
    this.duration = duration;
  }

  @Override
  public void evictTables(AccessCountTableDeque tables, int size) {
    if (tables.peek() != null){
      AccessCountTable latestTable = tables.peekLast();
      Long threshHold = latestTable.getEndTime() - duration;
      for (Iterator<AccessCountTable> iterator = tables.iterator(); iterator.hasNext();) {
        AccessCountTable table = iterator.next();
        if (table.getStartTime() < threshHold) {
          this.dropTable(table);
          iterator.remove();
        } else {
          break;
        }
      }
    }
  }
}
