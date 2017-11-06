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

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.metrics.FileAccessEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class AccessEventAggregator {
  private final MetaStore adapter;
  private final long aggregationGranularity;
  private final AccessCountTableManager accessCountTableManager;
  private Window currentWindow;
  private List<FileAccessEvent> eventBuffer;
  private Map<String, Integer> lastAccessCount = new HashMap<>();
  public static final Logger LOG =
      LoggerFactory.getLogger(AccessEventAggregator.class);

  public AccessEventAggregator(MetaStore adapter, AccessCountTableManager manager) {
    this(adapter, manager,  5 * 1000L);
  }

  public AccessEventAggregator(MetaStore adapter,
      AccessCountTableManager manager, long aggregationGranularity) {
    this.adapter = adapter;
    this.accessCountTableManager = manager;
    this.aggregationGranularity = aggregationGranularity;
    this.eventBuffer = new ArrayList<>();
  }

  public void addAccessEvents(List<FileAccessEvent> eventList) {
    if (this.currentWindow == null && !eventList.isEmpty()) {
      this.currentWindow = assignWindow(eventList.get(0).getTimestamp());
    }
    for (FileAccessEvent event : eventList) {
      if (!this.currentWindow.contains(event.getTimestamp())) {
        // New Window occurs
        this.createTable();
        this.currentWindow = assignWindow(event.getTimestamp());
        this.eventBuffer.clear();
      }
      // Exclude watermark event
      if (!event.getPath().isEmpty()) {
        this.eventBuffer.add(event);
      }
    }
  }

  private void createTable() {
    AccessCountTable table = new AccessCountTable(currentWindow.start, currentWindow.end);
    String createTable = AccessCountDao.createAccessCountTableSQL(table.getTableName());
    try {
      adapter.execute(createTable);
      adapter.insertAccessCountTable(table);
    } catch (MetaStoreException e) {
      LOG.error("Create table error: " + table, e);
      return;
    }
    if (this.eventBuffer.size() > 0 || lastAccessCount.size() > 0) {
      Map<String, Integer> accessCount = this.getAccessCountMap(eventBuffer);
      Set<String> now = new HashSet<>();
      now.addAll(accessCount.keySet());
      accessCount = mergeMap(accessCount, lastAccessCount);

      final Map<String, Long> pathToIDs;
      try {
        pathToIDs = adapter.getFileIDs(accessCount.keySet());
      } catch (MetaStoreException e) {
        // TODO: dirty handle here
        LOG.error("Create Table " + table.getTableName(), e);
        return;
      }

      now.removeAll(pathToIDs.keySet());
      Map<String, Integer> tmpLast = new HashMap<>();
      for (String key : now) {
        tmpLast.put(key, accessCount.get(key));
      }

      List<String> values = new ArrayList<>();
      for (String key : pathToIDs.keySet()) {
        values.add(String.format("(%d, %d)", pathToIDs.get(key),
            accessCount.get(key)));
      }

      if (LOG.isDebugEnabled()) {
        if (lastAccessCount.size() != 0) {
          Set<String> non = lastAccessCount.keySet();
          non.removeAll(pathToIDs.keySet());
          if (non.size() != 0) {
            String result = "Access events ignored for file:\n";
            for (String p : non) {
              result += p + " --> " + lastAccessCount.get(p) + "\n";
            }
            LOG.debug(result);
          }
        }
      }
      lastAccessCount = tmpLast;

      if (values.size() != 0) {
        String insertValue = String.format(
            "INSERT INTO %s (%s, %s) VALUES %s",
            table.getTableName(),
            AccessCountDao.FILE_FIELD,
            AccessCountDao.ACCESSCOUNT_FIELD,
            StringUtils.join(values, ", "));
        try {
          this.adapter.execute(insertValue);
          this.adapter.updateCachedFiles(pathToIDs, eventBuffer);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Table created: " + table);
          }
        } catch (MetaStoreException e) {
          LOG.error("Create table error: " + table, e);
        }
      }
    }
    this.accessCountTableManager.addTable(table);
  }

  private Map<String, Integer> mergeMap(Map<String, Integer> map1, Map<String, Integer> map2) {
    for (Entry<String, Integer> entry : map2.entrySet()) {
      String key = entry.getKey();
      if (map1.containsKey(key)) {
        map1.put(key, map1.get(key) + entry.getValue());
      } else {
        map1.put(key, map2.get(key));
      }
    }
    return map1;
  }

  private Map<String, Integer> getAccessCountMap(List<FileAccessEvent> events) {
    Map<String, Integer> map = new HashMap<>();
    for (FileAccessEvent event : events) {
      String path = event.getPath();
      if (map.containsKey(path)) {
        map.put(path, map.get(path) + 1);
      } else {
        map.put(path, 1);
      }
    }
    return map;
  }

  private Window assignWindow(long time) {
    long start = time - (time % aggregationGranularity);
    return new Window(start, start + aggregationGranularity);
  }

  private class Window {
    private long start;
    private long end;

    public Window(long start, long end) {
      this.start = start;
      this.end = end;
    }

    // [start, end)
    public boolean contains(long time) {
      return this.start <= time && this.end > time;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Window)) {
        return false;
      } else {
        Window other = (Window) o;
        return this.start == other.start && this.end == other.end;
      }
    }
  }
}
