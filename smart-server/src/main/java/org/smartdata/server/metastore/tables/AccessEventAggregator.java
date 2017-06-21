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

import org.apache.commons.lang.StringUtils;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.server.metastore.MetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AccessEventAggregator {
  private final MetaStore adapter;
  private final long aggregationGranularity;
  private final AccessCountTableManager accessCountTableManager;
  private Window currentWindow;
  private List<FileAccessEvent> eventBuffer;
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
      if (this.currentWindow.contains(event.getTimestamp())) {
        // Exclude watermark event
        if (!event.getPath().isEmpty()) {
          this.eventBuffer.add(event);
        }
      } else { // New Window occurs
        this.createTable();
        this.currentWindow = assignWindow(event.getTimestamp());
        this.eventBuffer.clear();
        if (!event.getPath().isEmpty()) {
          this.eventBuffer.add(event);
        }
      }
    }
  }

  private void createTable() {
    AccessCountTable table = new AccessCountTable(currentWindow.start, currentWindow.end);
    String createTable = AccessCountTable.createTableSQL(table.getTableName());
    try {
      this.adapter.execute(createTable);
    } catch (SQLException e) {
      LOG.error("Create table error: " + table, e);
    }
    if (this.eventBuffer.size() > 0) {
      final Map<String, Long> pathToIDs;
      try {
        pathToIDs = adapter.getFileIDs(getPaths(eventBuffer));
      } catch (SQLException e) {
        // TODO: dirty handle here
        LOG.error("Create Table " + table.getTableName(), e);
        return;
      }
      Map<String, Integer> accessCount = this.getAccessCountMap(eventBuffer);
      List<String> values = new ArrayList<>();
      for(Map.Entry<String, Integer> entry: accessCount.entrySet()) {
        values.add("(" + pathToIDs.get(entry.getKey()) + ", " + entry.getValue() + ")");
      }

      String insertValue = String.format(
        "INSERT INTO %s (%s, %s) VALUES %s",
        table.getTableName(),
        AccessCountTable.FILE_FIELD,
        AccessCountTable.ACCESSCOUNT_FIELD,
        StringUtils.join(values, ", "));
      try {
        this.adapter.execute(insertValue);
        this.adapter.updateCachedFiles(pathToIDs, eventBuffer);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Table created: " + table);
        }
      } catch (SQLException e) {
        LOG.error("Create table error: " + table, e);
      }
    }
    this.accessCountTableManager.addTable(table);
  }

  private Set<String> getPaths(List<FileAccessEvent> events) {
    Set<String> paths = new HashSet<>();
    for (FileAccessEvent event : events) {
      paths.add(event.getPath());
    }
    return paths;
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
