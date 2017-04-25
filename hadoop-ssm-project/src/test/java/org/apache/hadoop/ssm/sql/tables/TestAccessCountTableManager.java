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

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TestAccessCountTableManager {

  @Test
  public void testAccessCountTableManager() {
    AccessCountTableManager manager = new AccessCountTableManager();
    Long firstDayEnd = 24 * 60 * 60 * 1000L;
    AccessCountTable accessCountTable = new AccessCountTable(firstDayEnd - 5 * 1000,
      firstDayEnd, TimeGranularity.SECOND);
    manager.addSecondTable(accessCountTable);

    Map<TimeGranularity, AccessCountTableList> map = manager.getTableLists();
    AccessCountTableList second = map.get(TimeGranularity.SECOND);
    Assert.assertTrue(second.size() == 1);
    Assert.assertEquals(second.get(0), accessCountTable);

    AccessCountTableList minute = map.get(TimeGranularity.MINUTE);
    AccessCountTable minuteTable = new AccessCountTable(firstDayEnd - 60 * 1000,
      firstDayEnd, TimeGranularity.MINUTE);
    Assert.assertTrue(minute.size() == 1);
    Assert.assertEquals(minute.get(0), minuteTable);

    AccessCountTableList hour = map.get(TimeGranularity.HOUR);
    AccessCountTable hourTable = new AccessCountTable(firstDayEnd - 60 *60 * 1000,
      firstDayEnd, TimeGranularity.HOUR);
    Assert.assertTrue(hour.size() == 1);
    Assert.assertEquals(hour.get(0), hourTable);

    AccessCountTableList day = map.get(TimeGranularity.DAY);
    AccessCountTable dayTable = new AccessCountTable(firstDayEnd - 24 * 60 *60 * 1000,
      firstDayEnd, TimeGranularity.DAY);
    Assert.assertTrue(day.size() == 1);
    Assert.assertEquals(day.get(0), dayTable);
  }
}
