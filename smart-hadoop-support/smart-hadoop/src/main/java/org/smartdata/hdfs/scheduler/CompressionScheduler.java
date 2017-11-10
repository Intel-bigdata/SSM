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
package org.smartdata.hdfs.scheduler;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.SmartFileCompressionInfo;
import org.smartdata.model.action.ScheduleResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CompressionScheduler.
 */
public class CompressionScheduler extends ActionSchedulerService {
  static final Logger LOG =
      LoggerFactory.getLogger(CompressionScheduler.class);

  private static final List<String> actions = Arrays.asList("compress");

  private MetaStore metaStore;
  private Map<String, Long> fileLock;

  public CompressionScheduler(SmartContext context, MetaStore metaStore) {
    super(context, metaStore);
    this.metaStore = metaStore;
  }

  @Override
  public void init() throws IOException {
    this.fileLock = new ConcurrentHashMap<>();
  }

  @Override
  public void start() throws IOException {
  }

  @Override
  public void stop() throws IOException {
  }

  @Override
  public List<String> getSupportedActions() {
    return actions;
  }

  private boolean isFileLocked(String path) {
    if (fileLock.containsKey(path)) {
      LOG.warn("File " + path + " is locked.");
      return true;
    }
    return false;
  }

  @Override
  public boolean onSubmit(ActionInfo actionInfo) {
    String path = actionInfo.getArgs().get("-file");
    if (isFileLocked(path)) {
      return false;
    }
    try {
      if (metaStore.getCompressionInfo(path) != null) {
        LOG.warn("File " + path + " is already compressed, action abandoned");
        return false;
      }
    } catch (MetaStoreException e) {
      LOG.error("Compress action of file " + path + " failed in metastore!");
    }
    return true;
  }

  @Override
  public ScheduleResult onSchedule(ActionInfo actionInfo, LaunchAction action) {
    String path = actionInfo.getArgs().get("-file");
    long aid = action.getActionId();
    if (isFileLocked(path)) {
      return ScheduleResult.FAIL;
    }
    fileLock.put(path, aid);
    return ScheduleResult.SUCCESS;
  }

  @Override
  public void onActionFinished(ActionInfo actionInfo) {
    if (actionInfo.isFinished()) {
      try {
        Gson gson = new Gson();
        String compressionInfoJson = actionInfo.getResult();
        SmartFileCompressionInfo compressionInfo = gson.fromJson(compressionInfoJson,
            new TypeToken<SmartFileCompressionInfo>() {
            }.getType());
        metaStore.insertCompressedFile(compressionInfo);
        fileLock.remove(compressionInfo.getFileName());
      } catch (MetaStoreException e) {
        LOG.error("Compression action failed in metastore!", e);
      } catch (Exception e) {
        LOG.error("Compression action error", e);
      }
    }
  }
}