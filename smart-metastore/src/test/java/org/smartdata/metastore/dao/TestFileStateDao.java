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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.metastore.TestDaoUtil;
import org.smartdata.model.FileState;
import org.springframework.dao.EmptyResultDataAccessException;

import java.util.List;

public class TestFileStateDao extends TestDaoUtil {
  private FileStateDao fileStateDao;

  @Before
  public void initFileDao() throws Exception {
    initDao();
    fileStateDao = new FileStateDao(druidPool.getDataSource());
  }

  @Test
  public void testInsertUpdate() throws Exception {
    FileState fileState1 = new FileState("/file1", FileState.FileType.COMPACT,
        FileState.FileStage.PROCESSING);
    FileState fileState2 = new FileState("/file2", FileState.FileType.COMPRESSION,
        FileState.FileStage.DONE);
    fileStateDao.insertUpate(fileState1);
    fileStateDao.insertUpate(fileState2);
    List<FileState> fileStates = fileStateDao.getAll();
    Assert.assertEquals(2, fileStates.size());
    Assert.assertEquals(fileState1, fileStateDao.getByPath("/file1"));
    Assert.assertEquals(fileState2, fileStateDao.getByPath("/file2"));

    fileState1 = new FileState("/file1", FileState.FileType.COMPACT,
        FileState.FileStage.DONE);
    fileStateDao.insertUpate(fileState1);
    fileStates = fileStateDao.getAll();
    Assert.assertEquals(2, fileStates.size());
    Assert.assertEquals(fileState1, fileStateDao.getByPath("/file1"));
    Assert.assertEquals(fileState2, fileStateDao.getByPath("/file2"));
  }

  @Test
  public void testDelete() throws Exception {
    FileState fileState1 = new FileState("/file1", FileState.FileType.COMPACT,
        FileState.FileStage.PROCESSING);
    FileState fileState2 = new FileState("/file2", FileState.FileType.COMPRESSION,
        FileState.FileStage.DONE);
    FileState fileState3 = new FileState("/file3", FileState.FileType.S3,
        FileState.FileStage.DONE);
    fileStateDao.insertUpate(fileState1);
    fileStateDao.insertUpate(fileState2);
    fileStateDao.insertUpate(fileState3);

    fileStateDao.deleteByPath(fileState1.getPath());
    List<FileState> fileStates = fileStateDao.getAll();
    Assert.assertEquals(2, fileStates.size());
    try {
      fileStateDao.getByPath(fileState1.getPath());
      Assert.fail();
    } catch (EmptyResultDataAccessException e) {
      // It is correct if no entry found
    }

    fileStateDao.deleteAll();
    fileStates = fileStateDao.getAll();
    Assert.assertEquals(0, fileStates.size());
  }
}
