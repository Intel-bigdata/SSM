/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ssm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

/**
 * Created by cc on 17-3-1.
 */
public class CacheStatusReport {
  private Configuration conf;
  private Map<String, List<CacheStatus.cacheFileInfo>> reportMap;

  public CacheStatusReport() {
    conf = new Configuration();
    reportMap = new HashMap<>();
  }

  /**
   * getCacheStatusReport : Get a CacheStatus report
   */
  public CacheStatus getCacheStatusReport() throws IOException {
    DFSClient dfsClient = new DFSClient(conf);
    CacheStatus cacheStatus = new CacheStatus();
    long cacheCapacityTotal = 0;
    long cacheUsedTotal = 0;
    long cacheRemaTotal = 0;
    float cacheUsedPerTotal = 0;
    int len = dfsClient.getDatanodeStorageReport(HdfsConstants.DatanodeReportType.LIVE).length;
    //get info from each dataNode
    for (int i = 0; i < len; i++) {
      cacheCapacityTotal += dfsClient.getDatanodeStorageReport(HdfsConstants.
              DatanodeReportType.LIVE)[i].getDatanodeInfo().getCacheCapacity();
      cacheUsedTotal += dfsClient.getDatanodeStorageReport(HdfsConstants.
              DatanodeReportType.LIVE)[i].getDatanodeInfo().getCacheUsed();
      cacheRemaTotal += dfsClient.getDatanodeStorageReport(HdfsConstants.
              DatanodeReportType.LIVE)[i].getDatanodeInfo().getCacheRemaining();
      cacheUsedPerTotal += dfsClient.getDatanodeStorageReport(HdfsConstants.
              DatanodeReportType.LIVE)[i].getDatanodeInfo().getCacheUsedPercent();
    }
    cacheStatus.setCacheCapacity(cacheCapacityTotal);
    cacheStatus.setCacheUsed(cacheUsedTotal);
    cacheStatus.setCacheRemaining(cacheRemaTotal);
    cacheStatus.setCacheUsedPercentage(cacheUsedPerTotal);
    
    //get the cacheStatusMap
    String poolName;
    String path;
    Short replication;
    CacheStatus.cacheFileInfo cacheFileInfo = null;
    while (dfsClient.listCacheDirectives(null).hasNext()) {
      poolName = dfsClient.listCacheDirectives(null).next().getInfo().getPool();
      path = dfsClient.listCacheDirectives(null).next().getInfo().getPath().toString();
      replication = dfsClient.listCacheDirectives(null).next().getInfo().getReplication();
      if (reportMap.containsKey(poolName)) {
        List<CacheStatus.cacheFileInfo> list = reportMap.get(poolName);
        cacheFileInfo.setFilePath(path);
        cacheFileInfo.setRepliNum(replication);
        list.add(cacheFileInfo);
        reportMap.put(poolName, list);
      } else {
        cacheFileInfo.setFilePath(path);
        cacheFileInfo.setRepliNum(replication);
        List<CacheStatus.cacheFileInfo> list = new ArrayList<>();
        list.add(cacheFileInfo);
        reportMap.put(poolName, list);
      }
    }
    cacheStatus.setCacheStatusMap(reportMap);
    return cacheStatus;
  }
}