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
package org.smartdata.metastore.tables;

import org.smartdata.metastore.MetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.metastore.MetaStoreException;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public class AccessCountTableAggregator {
  private final MetaStore adapter;
  public static final Logger LOG =
      LoggerFactory.getLogger(AccessCountTableAggregator.class);

  public AccessCountTableAggregator(MetaStore adapter) {
    this.adapter = adapter;
  }

  public void aggregate(AccessCountTable destinationTable,
      List<AccessCountTable> tablesToAggregate) throws MetaStoreException {
    if (tablesToAggregate.size() > 0) {
      String aggregateSQ = adapter.aggregateSQLStatement(destinationTable, tablesToAggregate);
      this.adapter.execute(aggregateSQ);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(tablesToAggregate.size() + " tables aggregated into " + destinationTable);
      for (AccessCountTable table : tablesToAggregate) {
        LOG.debug("\t" + table);
      }
    }
  }

}
