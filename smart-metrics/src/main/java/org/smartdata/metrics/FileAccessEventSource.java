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
package org.smartdata.metrics;

/**
 * This interface aims to collect file access event through different ways.
 */
public interface FileAccessEventSource {
  /**
   * Get a collector what will produce events from this file access event source.
   */
  FileAccessEventCollector getCollector();

  /**
   * Insert events generated from the Smart client so that the collector can consume.
   * The actual implementation of FileAccessEventSource doesn't have to support this.
   * @param event The event that generated from Smart client
   */
  void insertEventFromSmartClient(FileAccessEvent event);

  /**
   * Close the source, release resources if necessary.
   */
  void close();
}
