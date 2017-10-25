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

import com.google.common.collect.Lists;
import org.junit.Test;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.metrics.FileAccessEvent;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestAccessEventAggregator {

  @Test
  public void testAccessEventAggregator() throws MetaStoreException {
    MetaStore adapter = mock(MetaStore.class);
    AccessCountTableManager manager = mock(AccessCountTableManager.class);
    AccessEventAggregator aggregator = new AccessEventAggregator(adapter, manager);

    aggregator.addAccessEvents(Lists.newArrayList(new FileAccessEvent("", 3000)));
    verify(adapter, never()).execute(anyString());

    aggregator.addAccessEvents(Lists.newArrayList(new FileAccessEvent("", 6000)));
    verify(adapter, times(1)).execute(anyString());
    verify(manager, times(1)).addTable(any(AccessCountTable.class));

    aggregator.addAccessEvents(
        Lists.newArrayList(
            new FileAccessEvent("abc", 8000),
            new FileAccessEvent("def", 14000),
            new FileAccessEvent("", 18000)));

    verify(adapter, times(3)).execute(anyString());
    verify(manager, times(3)).addTable(any(AccessCountTable.class));
  }
}
