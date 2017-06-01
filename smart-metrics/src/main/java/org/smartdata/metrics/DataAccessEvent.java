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
 * A data access event. It means ONE access to a piece of data like a file (in a file system) or
 * an object (in object store).
 */
public interface DataAccessEvent {

  /**
   * Get the user name who accessed the data.
   * @return user name
   */
  String getAccessedBy();

  /**
   * Get the access time.
   * @return access time
   */
  long getTimestamp();
}
