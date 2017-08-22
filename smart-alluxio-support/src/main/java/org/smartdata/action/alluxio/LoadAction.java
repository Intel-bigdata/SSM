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
package org.smartdata.action.alluxio;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;

public class LoadAction extends AlluxioAction {

  private List<String> exceptionMessages;
  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.actionType = AlluxioActionType.LOAD;
    this.exceptionMessages = new ArrayList<>();
  }

  @Override
  protected void execute() throws Exception {
    LOG.info("Executing Alluxio action: LoadAction, path:" + uri.toString());
    loadInternal(uri);
    if (!exceptionMessages.isEmpty()) {
      for (String message : exceptionMessages) {
        LOG.warn(message);
      }
    } else {
      LOG.info("Path " + uri + " was successfully loaded.");
    }
  }

  // load file into memory recursively
  private void loadInternal(AlluxioURI path) throws Exception {
    URIStatus status = alluxioFs.getStatus(path);
    if (status.isFolder()) {
      List<URIStatus> statuses = alluxioFs.listStatus(path);
      for (URIStatus uriStatus : statuses) {
        AlluxioURI newPath = new AlluxioURI(uriStatus.getPath());
        loadInternal(newPath);
      }
    } else {
      if (status.getInMemoryPercentage() == 100) {
        // The file has already been fully loaded into Alluxio memory.
        return;
      }
      FileInStream in = null;
      try {
        OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.CACHE_PROMOTE);
        in = alluxioFs.openFile(path, options);
        byte[] buf = new byte[8 * Constants.MB];
        while (in.read(buf) != -1) {
        }
      } catch (Exception e) {
        exceptionMessages.add(e.getMessage());
      } finally {
        if (null != in){
          in.close();
        }
      }
    }
  }
}
