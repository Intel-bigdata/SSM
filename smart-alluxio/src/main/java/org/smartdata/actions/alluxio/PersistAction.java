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
package org.smartdata.actions.alluxio;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemUtils;
import alluxio.client.file.URIStatus;

public class PersistAction extends AlluxioAction {
  private List<String> exceptionMessages;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.actionType = AlluxioActionType.PERSIST;
    this.exceptionMessages = new ArrayList<>();
  }

  @Override
  protected void execute() {
    try {
      LOG.info("Executing Alluxio action: PersistAction, path:"
          + uri.toString());
      persistInternal(uri);
      if (!exceptionMessages.isEmpty()) {
        for (String message : exceptionMessages) {
          LOG.warn(message);
        }
      } else {
        LOG.info("Path " + uri + " was successfully persisted.");
      }
    } catch (Exception e) {
      actionStatus.setSuccessful(false);
      throw new RuntimeException(e);
    } finally {
      actionStatus.end();
    }
  }

  // persist file to underfilesystem recursively
  private void persistInternal(AlluxioURI path) throws Exception {
    URIStatus status = alluxioFs.getStatus(path);
    if (status.isFolder()) {
      List<URIStatus> statuses = alluxioFs.listStatus(path);
      for (URIStatus uriStatus : statuses) {
        AlluxioURI newPath = new AlluxioURI(uriStatus.getPath());
        persistInternal(newPath);
      }
    } else if (status.isPersisted()) {
      LOG.info(path + " is already persisted.");
    } else {
      try {
        FileSystemUtils.persistFile(alluxioFs, path);
        LOG.info("Persisted file " + path + " with size " + status.getLength());
      } catch (Exception e) {
        exceptionMessages.add(e.getMessage());
      }
    }
  }
}