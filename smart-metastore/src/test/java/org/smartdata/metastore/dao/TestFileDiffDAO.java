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
import org.smartdata.model.FileDiff;
import org.smartdata.model.FileDiffType;
import org.smartdata.model.FileInfo;
import org.smartdata.metastore.utils.TestDaoUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TestFileDiffDAO extends TestDaoUtil{
  private FileDiffDAO fileDiffDAO;

  @Before
  public void initFileDiffDAO() throws Exception {
    initDao();
    fileDiffDAO = new FileDiffDAO(druidPool.getDataSource());
  }

  @After
  public void closeFileDiffDAO() throws Exception {
    closeDao();
    fileDiffDAO = null;
  }

  @Test
  public void testInsertAndGetSingleRecord() {
    FileDiff fileDiff = new FileDiff();
    fileDiff.setDiffId(1);
    fileDiff.setParameters("test");
    fileDiff.setApplied(true);
    fileDiff.setDiffType(FileDiffType.APPEND);
    fileDiff.setCreate_time(1);
    fileDiffDAO.insert(fileDiff);
    Assert.assertTrue(fileDiffDAO.getById(1).equals(fileDiff));
  }

  @Test
  public void testBatchInsertAndQuery() {
    FileDiff[] fileDiffs = new FileDiff[2];
    fileDiffs[0] = new FileDiff();
    fileDiffs[0].setDiffId(1);
    fileDiffs[0].setParameters("test");
    fileDiffs[0].setApplied(true);
    fileDiffs[0].setDiffType(FileDiffType.APPEND);
    fileDiffs[0].setCreate_time(1);

    fileDiffs[1] = new FileDiff();
    fileDiffs[1].setDiffId(2);
    fileDiffs[1].setParameters("test");
    fileDiffs[1].setApplied(true);
    fileDiffs[1].setDiffType(FileDiffType.APPEND);
    fileDiffs[1].setCreate_time(1);

    fileDiffDAO.insert(fileDiffs);
    List<FileDiff> fileInfoList = fileDiffDAO.getALL();
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(fileInfoList.get(i).equals(fileDiffs[i]));
    }
  }

  @Test
  public void testUpdate() {
    FileDiff fileDiff = new FileDiff();
    fileDiff.setDiffId(1);
    fileDiff.setParameters("test");
    fileDiff.setApplied(true);
    fileDiff.setDiffType(FileDiffType.APPEND);
    fileDiff.setCreate_time(1);
    fileDiffDAO.insert(fileDiff);

    fileDiffDAO.update(1,false);
    fileDiff.setApplied(false);

    Assert.assertTrue(fileDiffDAO.getById(1).equals(fileDiff));
  }
}
