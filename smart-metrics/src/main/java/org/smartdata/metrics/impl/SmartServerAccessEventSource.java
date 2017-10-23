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
package org.smartdata.metrics.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.metrics.FileAccessEventCollector;
import org.smartdata.metrics.FileAccessEventSource;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The default AccessEventSource for SmartServer.
 */
public class SmartServerAccessEventSource implements FileAccessEventSource {
  static final Logger LOG = LoggerFactory.getLogger(SmartServerAccessEventSource.class);

  private static final long DEFAULT_INTERVAL = 1000; //5 seconds
  private final SmartServerAccessEventCollector collector;
  private LinkedBlockingQueue<FileAccessEvent> eventQueue;
  private Timer timer;

  public SmartServerAccessEventSource() {
    this.timer = new Timer();
    this.eventQueue = new LinkedBlockingQueue<>();
    this.collector = new SmartServerAccessEventCollector(eventQueue);
    this.timer.schedule(new ProgressInsertTask(eventQueue), DEFAULT_INTERVAL, DEFAULT_INTERVAL);
  }

  @Override
  public FileAccessEventCollector getCollector() {
    return this.collector;
  }

  @Override
  public void insertEventFromSmartClient(FileAccessEvent event) {
    try {
      this.eventQueue.put(event);
      LOG.trace("Access:" + event.getPath());
    } catch (InterruptedException e) {
      LOG.error("Event queue enqueue path={} error", event.getPath(), e);
    }
  }

  @Override
  public void close() {
    this.timer.cancel();
  }

  private static class ProgressInsertTask extends TimerTask {
    private final LinkedBlockingQueue<FileAccessEvent> outerQueue;

    public ProgressInsertTask(LinkedBlockingQueue<FileAccessEvent> outerQueue) {
      this.outerQueue = outerQueue;
    }

    @Override
    public void run() {
      try {
        //Todo: do not use HDFSFileAccessEvent
        this.outerQueue.put(new FileAccessEvent("", System.currentTimeMillis()));
      } catch (InterruptedException e) {
        LOG.error("Outer queue enqueue error", e);
      }
    }
  }
}
