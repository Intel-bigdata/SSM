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
package org.smartdata.metastore.dao;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.metastore.TestDaoUtil;
import org.smartdata.model.SmartFileCompressionInfo;

import java.util.ArrayList;
import java.util.List;

public class TestCompressionFileDao extends TestDaoUtil {

  private CompressionFileDao compressionFileDao;
  private List<Long> originalPos = new ArrayList<>();
  private List<Long> compressedPos = new ArrayList<>();

  @Before
  public void initCompressionFileDao() throws Exception {
    initDao();
    compressionFileDao = new CompressionFileDao(druidPool.getDataSource());
    originalPos.add(9000L);
    originalPos.add(8000L);
    compressedPos.add(3000L);
    compressedPos.add(2000L);
  }

  @After
  public void closeCacheFileDao() throws Exception {
    closeDao();
    compressionFileDao = null;
  }

  @Test
  public void testInsertDeleteCompressionFiles() throws Exception {
    SmartFileCompressionInfo compressionInfo = new SmartFileCompressionInfo(
      "/test", 131072, originalPos, compressedPos);

    //insert test
    compressionFileDao.insert(compressionInfo);
    Assert.assertTrue(compressionFileDao.getInfoByName("/test").
      getOriginalPos().get(0).equals(9000L));

    //delete test
    compressionFileDao.deleteByName("/test");
    Assert.assertTrue(compressionFileDao.getAll().size() == 0);
  }

  @Test
  public void testGetCompressionInfo() throws Exception {
    SmartFileCompressionInfo compressionInfo = new SmartFileCompressionInfo(
      "/test1", 131072, originalPos, compressedPos);
    SmartFileCompressionInfo compressionInfo2 = new SmartFileCompressionInfo(
      "/test2", 131072, originalPos, compressedPos);

    compressionFileDao.insert(compressionInfo);
    compressionFileDao.insert(compressionInfo2);
    SmartFileCompressionInfo dbcompressionInfo = compressionFileDao.getInfoByName("/test1");

    Assert.assertTrue(dbcompressionInfo.getFileName().equals("/test1"));
    Assert.assertTrue(dbcompressionInfo.getBufferSize() == 131072);
    Assert.assertTrue(dbcompressionInfo.getOriginalPos().get(0).equals(9000L));
    Assert.assertTrue(dbcompressionInfo.getCompressedPos().get(1).equals(2000L));
    Assert.assertTrue(compressionFileDao.getAll().size() == 2);
  }
}
