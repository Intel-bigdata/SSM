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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.metastore.TestDaoUtil;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.model.CachedFileStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestCacheFileDao extends TestDaoUtil {

  private CacheFileDao cacheFileDao;

  @Before
  public void initCacheFileDao() throws Exception {
    initDao();
    cacheFileDao = new CacheFileDao(druidPool.getDataSource());
  }

  @After
  public void closeCacheFileDao() throws Exception {
    closeDao();
    cacheFileDao = null;
  }

  @Test
  public void testUpdateCachedFiles() throws Exception {
    CachedFileStatus first = new CachedFileStatus(80L,
        "testPath", 1000L, 2000L, 100);
    cacheFileDao.insert(first);
    CachedFileStatus second = new CachedFileStatus(90L,
        "testPath2", 2000L, 3000L, 200);
    cacheFileDao.insert(second);
    Map<String, Long> pathToId = new HashMap<>();
    pathToId.put("testPath", 80L);
    pathToId.put("testPath2", 90L);
    pathToId.put("testPath3", 100L);
    List<FileAccessEvent> events = new ArrayList<>();
    events.add(new FileAccessEvent("testPath", 3000L));
    events.add(new FileAccessEvent("testPath", 4000L));
    events.add(new FileAccessEvent("testPath2", 4000L));
    events.add(new FileAccessEvent("testPath2", 5000L));
    events.add(new FileAccessEvent("testPath3", 8000L));
    events.add(new FileAccessEvent("testPath3", 9000L));
    // Sync status
    first.setLastAccessTime(4000L);
    first.setNumAccessed(first.getNumAccessed() + 2);
    second.setLastAccessTime(5000L);
    second.setNumAccessed(second.getNumAccessed() + 2);
    cacheFileDao.update(pathToId, events);
    List<CachedFileStatus> statuses = cacheFileDao.getAll();
    Assert.assertTrue(statuses.size() == 2);
    Map<Long, CachedFileStatus> statusMap = new HashMap<>();
    for (CachedFileStatus status : statuses) {
      statusMap.put(status.getFid(), status);
    }
    Assert.assertTrue(statusMap.containsKey(80L));
    CachedFileStatus dbFirst = statusMap.get(80L);
    Assert.assertTrue(dbFirst.equals(first));
    Assert.assertTrue(statusMap.containsKey(90L));
    CachedFileStatus dbSecond = statusMap.get(90L);
    Assert.assertTrue(dbSecond.equals(second));
  }

  @Test
  public void testInsertDeleteCachedFiles() throws Exception {
    cacheFileDao
        .insert(80L,
            "testPath", 123456L, 234567L, 456);
    Assert.assertTrue(cacheFileDao.getById(
      80L).getFromTime() == 123456L);
    // Update record with 80l id
    cacheFileDao.update(80L,
      123455L, 460);
    Assert.assertTrue(cacheFileDao
                          .getAll().get(0)
                          .getLastAccessTime() == 123455L);
    CachedFileStatus[] cachedFileStatuses = new CachedFileStatus[] {
        new CachedFileStatus(321L, "testPath",
          113334L, 222222L, 222)};
    cacheFileDao.insert(cachedFileStatuses);
    Assert.assertTrue(cacheFileDao.getById(321L)
                          .equals(cachedFileStatuses[0]));
    Assert.assertTrue(cacheFileDao.getAll().size() == 2);
    // Delete one record
    cacheFileDao.deleteById(321L);
    Assert.assertTrue(cacheFileDao.getAll().size() == 1);
    // Clear all records
    cacheFileDao.deleteAll();
    Assert.assertTrue(cacheFileDao.getAll().size() == 0);
  }

  @Test
  public void testGetCachedFileStatus() throws Exception {
    cacheFileDao.insert(6L, "testPath", 1490918400000L,
      234567L, 456);
    CachedFileStatus cachedFileStatus = new CachedFileStatus(6L, "testPath", 1490918400000L,
      234567L, 456);
    cacheFileDao.insert(19L, "testPath", 1490918400000L,
      234567L, 456);
    cacheFileDao.insert(23L, "testPath", 1490918400000L,
      234567L, 456);
    CachedFileStatus dbcachedFileStatus = cacheFileDao.getById(6);
    Assert.assertTrue(dbcachedFileStatus.equals(cachedFileStatus));
    List<CachedFileStatus> cachedFileList = cacheFileDao.getAll();
    List<Long> fids = cacheFileDao.getFids();
    Assert.assertTrue(fids.size() == 3);
    Assert.assertTrue(cachedFileList.get(0).getFid() == 6);
    Assert.assertTrue(cachedFileList.get(1).getFid() == 19);
    Assert.assertTrue(cachedFileList.get(2).getFid() == 23);
  }
}
