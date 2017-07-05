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
package org.smartdata.metastore;


import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.model.CmdletState;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CachedFileStatus;
import org.smartdata.model.FileAccessInfo;
import org.smartdata.model.FileInfo;
import org.smartdata.model.FileStatusInternal;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.StorageCapacity;
import org.smartdata.model.StoragePolicy;
import org.smartdata.model.RuleState;
import org.smartdata.metastore.dao.AccessCountTable;
import org.smartdata.metastore.dao.ActionDao;
import org.smartdata.metastore.dao.CacheFileDao;
import org.smartdata.metastore.dao.GroupsDao;
import org.smartdata.metastore.dao.RuleDao;
import org.smartdata.metastore.dao.StorageDao;
import org.smartdata.metastore.dao.CmdletDao;
import org.smartdata.metastore.dao.FileDao;
import org.smartdata.metastore.dao.UserDao;
import org.smartdata.metastore.dao.XattrDao;
import org.smartdata.metastore.utils.MetaStoreUtils;
import org.smartdata.metastore.dao.*;
import org.smartdata.metrics.FileAccessEvent;
import org.springframework.dao.EmptyResultDataAccessException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.smartdata.metastore.utils.MetaStoreUtils.getKey;


/**
 * Operations supported for upper functions.
 */
public class MetaStore {
  static final Logger LOG = LoggerFactory.getLogger(MetaStore.class);

  private DBPool pool = null;

  private Map<Integer, String> mapOwnerIdName = null;
  private Map<Integer, String> mapGroupIdName = null;
  private Map<Integer, String> mapStoragePolicyIdName = null;
  private Map<String, Integer> mapStoragePolicyNameId = null;
  private Map<String, StorageCapacity> mapStorageCapacity = null;
  private RuleDao ruleDao;
  private CmdletDao cmdletDao;
  private ActionDao actionDao;
  private FileDao fileDao;
  private FileInfoDao fileInfoDao;
  private CacheFileDao cacheFileDao;
  private StorageDao storageDao;
  private UserDao userDao;
  private GroupsDao groupsDao;
  private XattrDao xattrDao;
  private AccessCountDao accessCountDao;
  private MetaStoreHelper metaStoreHelper;

  public MetaStore(DBPool pool) throws MetaStoreException {
    this.pool = pool;
    ruleDao = new RuleDao(pool.getDataSource());
    cmdletDao = new CmdletDao(pool.getDataSource());
    actionDao = new ActionDao(pool.getDataSource());
    fileDao = new FileDao(pool.getDataSource());
    fileInfoDao = new FileInfoDao(pool.getDataSource());
    xattrDao = new XattrDao(pool.getDataSource());
    cacheFileDao = new CacheFileDao(pool.getDataSource());
    userDao = new UserDao(pool.getDataSource());
    storageDao = new StorageDao(pool.getDataSource());
    groupsDao = new GroupsDao(pool.getDataSource());
    accessCountDao = new AccessCountDao(pool.getDataSource());
    metaStoreHelper = new MetaStoreHelper(pool.getDataSource());
  }

  public Connection getConnection() throws MetaStoreException {
    if (pool != null) {
      try {
        return pool.getConnection();
      } catch (SQLException e) {
        throw new MetaStoreException(e);
      }
    }
    return null;
  }

  private void closeConnection(Connection conn) throws MetaStoreException {
    if (pool != null) {
      try {
        pool.closeConnection(conn);
      } catch (SQLException e) {
        throw new MetaStoreException(e);
      }
    }
  }

  public synchronized void addUser(String userName) throws MetaStoreException {
    try {
      userDao.addUser(userName);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized void addGroup(
      String groupName) throws MetaStoreException {
    try {
      groupsDao.addGroup(groupName);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  private void updateUsersMap() throws MetaStoreException {
    mapOwnerIdName = userDao.getUsersMap();
    fileDao.updateUsersMap(mapOwnerIdName);
  }

  private void updateGroupsMap() throws MetaStoreException {
    try {
      mapGroupIdName = groupsDao.getGroupsMap();
      fileDao.updateGroupsMap(mapGroupIdName);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  /**
   * Store files info into database.
   *
   * @param files
   */
  public synchronized void insertFiles(FileStatusInternal[] files)
      throws MetaStoreException {
    updateCache();
    for (FileStatusInternal file : files) {
      String owner = file.getOwner();
      String group = file.getGroup();
      if (!this.mapOwnerIdName.values().contains(owner)) {
        this.addUser(owner);
        this.updateUsersMap();
      }
      if (!this.mapGroupIdName.values().contains(group)) {
        this.addGroup(group);
        this.updateGroupsMap();
      }
    }
    try {
      fileDao.insert(files);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  /**
   * Store files info into database.
   *
   * @param files
   */
  public synchronized void insertFiles(FileInfo[] files)
      throws MetaStoreException {
    updateCache();
    for (FileInfo file: files) {
      String owner = file.getOwner();
      String group = file.getGroup();
      if (!this.mapOwnerIdName.values().contains(owner)) {
        this.addUser(owner);
        this.updateUsersMap();
      }
      if (!this.mapGroupIdName.values().contains(group)) {
        this.addGroup(group);
        this.updateGroupsMap();
      }
    }
    fileInfoDao.insert(files);
  }

  public int updateFileStoragePolicy(String path, String policyName)
      throws MetaStoreException {
    if (mapStoragePolicyIdName == null) {
      updateCache();
    }
    if (!mapStoragePolicyNameId.containsKey(policyName)) {
      throw new MetaStoreException("Unknown storage policy name '"
          + policyName + "'");
    }
    try {
      return storageDao.updateFileStoragePolicy(path, policyName);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<HdfsFileStatus> getFile() throws MetaStoreException {
    updateCache();
    try {
      return fileDao.getAll();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public HdfsFileStatus getFile(long fid) throws MetaStoreException {
    updateCache();
    try {
      return fileDao.getById(fid);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public Map<String, Long> getFileIDs(Collection<String> paths)
      throws MetaStoreException {
    try {
      return fileDao.getPathFids(paths);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public Map<Long, String> getFilePaths(Collection<Long> ids)
      throws MetaStoreException {
    try {
      return fileDao.getFidPaths(ids);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized List<FileAccessInfo> getHotFiles(
      List<AccessCountTable> tables,
      int topNum) throws MetaStoreException {
    Iterator<AccessCountTable> tableIterator = tables.iterator();
    if (tableIterator.hasNext()) {
      try{
        Map<Long, Integer> accessCounts = accessCountDao.getHotFiles(tables, topNum);
        if (accessCounts.size() == 0) {
          return new ArrayList<>();
        }
        Map<Long, String> idToPath = getFilePaths(accessCounts.keySet());
        List<FileAccessInfo> result = new ArrayList<>();
        for (Map.Entry<Long, Integer> entry : accessCounts.entrySet()) {
          Long fid = entry.getKey();
          if (idToPath.containsKey(fid)) {
            result.add(new FileAccessInfo(fid, idToPath.get(fid),
                accessCounts.get(fid)));
          }
        }
        return result;
      } catch (Exception e) {
        throw new MetaStoreException(e);
      } finally {
        for (AccessCountTable accessCountTable : tables) {
          if (accessCountTable.isView()) {
            this.dropView(accessCountTable.getTableName());
          }
        }
      }
    } else {
      return new ArrayList<>();
    }
  }


  public HdfsFileStatus getFile(String path) throws MetaStoreException {
    try {
      return fileDao.getByPath(path);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void insertStoragesTable(StorageCapacity[] storages)
      throws MetaStoreException {
    mapStorageCapacity = null;
    try {
      storageDao.insertStoragesTable(storages);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public StorageCapacity getStorageCapacity(
      String type) throws MetaStoreException {
    updateCache();
    try {
      return mapStorageCapacity.get(type);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized boolean updateStoragesTable(String type,
      Long capacity, Long free) throws MetaStoreException {
    try {
      return storageDao.updateStoragesTable(type, capacity, free);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  private void updateCache() throws MetaStoreException {
    if (mapOwnerIdName == null) {
      this.updateUsersMap();
    }

    if (mapGroupIdName == null) {
      this.updateGroupsMap();
    }
    if (mapStoragePolicyIdName == null) {
      mapStoragePolicyNameId = null;
      try {
        mapStoragePolicyIdName = storageDao.getStoragePolicyIdNameMap();
      } catch (Exception e) {
        throw new MetaStoreException(e);
      }
      mapStoragePolicyNameId = new HashMap<>();
      for (Integer key : mapStoragePolicyIdName.keySet()) {
        mapStoragePolicyNameId.put(mapStoragePolicyIdName.get(key), key);
      }
    }
    if (mapStorageCapacity == null) {
      try {
        mapStorageCapacity = storageDao.getStorageTablesItem();
      } catch (Exception e) {
        throw new MetaStoreException(e);
      }
    }
  }

  public synchronized void insertCachedFiles(long fid, String path,
      long fromTime,
      long lastAccessTime, int numAccessed) throws MetaStoreException {
    try {
      cacheFileDao.insert(fid, path, fromTime, lastAccessTime, numAccessed);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized void insertCachedFiles(List<CachedFileStatus> s)
      throws MetaStoreException {
    try {
      cacheFileDao.insert(s.toArray(new CachedFileStatus[s.size()]));
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void deleteAllCachedFile() throws MetaStoreException {
    try {
      cacheFileDao.deleteAll();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized boolean updateCachedFiles(Long fid,
      Long lastAccessTime, Integer numAccessed) throws MetaStoreException {
    try {
      return cacheFileDao.update(fid, lastAccessTime, numAccessed) >= 0;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void updateCachedFiles(Map<String, Long> pathToIds,
      List<FileAccessEvent> events)
      throws MetaStoreException {
    try {
      cacheFileDao.update(pathToIds, events);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void deleteCachedFile(long fid) throws MetaStoreException {
    try {
      cacheFileDao.deleteById(fid);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<CachedFileStatus> getCachedFileStatus() throws MetaStoreException {
    try {
      return cacheFileDao.getAll();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<Long> getCachedFids() throws MetaStoreException {
    try {
      return cacheFileDao.getFids();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public CachedFileStatus getCachedFileStatus(
      long fid) throws MetaStoreException {
    try {
      return cacheFileDao.getById(fid);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void createProportionView(AccessCountTable dest,
      AccessCountTable source)
      throws MetaStoreException {
    double percentage =
        ((double) dest.getEndTime() - dest.getStartTime())
            / (source.getEndTime() - source.getStartTime());
    String sql =
        String.format(
            "CREATE OR REPLACE VIEW %s AS SELECT %s, FLOOR(%s.%s * %s) AS %s FROM %s",
            dest.getTableName(),
            AccessCountDao.FILE_FIELD,
            source.getTableName(),
            AccessCountDao.ACCESSCOUNT_FIELD,
            percentage,
            AccessCountDao.ACCESSCOUNT_FIELD,
            source.getTableName());
    try {
      execute(sql);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void dropTable(String tableName) throws MetaStoreException {
    try {
      LOG.debug("Drop table = {}", tableName);
      metaStoreHelper.dropTable(tableName);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void dropView(String viewName) throws MetaStoreException {
    try {
      LOG.debug("Drop view = {}", viewName);
      metaStoreHelper.dropView(viewName);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void execute(String sql) throws MetaStoreException {
    try {
      LOG.debug("Execute sql = {}", sql);
      metaStoreHelper.execute(sql);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  //Todo: optimize
  public void execute(List<String> statements) throws MetaStoreException {
    for (String statement : statements) {
      execute(statement);
    }
  }

  public List<String> executeFilesPathQuery(
      String sql) throws MetaStoreException {
    try {
      return metaStoreHelper.getFilesPath(sql);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized boolean insertNewRule(RuleInfo info)
      throws MetaStoreException {
    try {
      return ruleDao.insert(info) >= 0;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized boolean updateRuleInfo(long ruleId, RuleState rs,
      long lastCheckTime, long checkedCount, int commandsGen)
      throws MetaStoreException {
    try {
      if (rs == null) {
        return ruleDao.update(ruleId,
            lastCheckTime, checkedCount, commandsGen) >= 0;
      }
      return ruleDao.update(ruleId,
          rs.getValue(), lastCheckTime, checkedCount, commandsGen) >= 0;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public RuleInfo getRuleInfo(long ruleId) throws MetaStoreException {
    try {
      return ruleDao.getById(ruleId);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<RuleInfo> getRuleInfo() throws MetaStoreException {
    try {
      return ruleDao.getAll();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized void insertCmdletsTable(CmdletInfo[] commands)
      throws MetaStoreException {
    try {
      cmdletDao.insert(commands);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized void insertCmdletTable(CmdletInfo command)
      throws MetaStoreException {
    try {
      cmdletDao.insert(command);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public long getMaxCmdletId() throws MetaStoreException {
    try {
      return cmdletDao.getMaxId();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public CmdletInfo getCmdletById(long cid) throws MetaStoreException {
    LOG.debug("Get cmdlet by cid {}", cid);
    try {
      return cmdletDao.getById(cid);
    } catch (EmptyResultDataAccessException e) {
      return null;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<CmdletInfo> getCmdletsTableItem(String cidCondition,
      String ridCondition, CmdletState state) throws MetaStoreException {
    try {
      return cmdletDao.getByCondition(cidCondition, ridCondition, state);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public boolean updateCmdletStatus(long cid, long rid, CmdletState state)
      throws MetaStoreException {
    try {
      return cmdletDao.update(cid, rid, state.getValue()) >= 0;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void deleteCmdlet(long cid) throws MetaStoreException {
    try {
      cmdletDao.delete(cid);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized void insertActionsTable(ActionInfo[] actionInfos)
      throws MetaStoreException {
    try {
      actionDao.insert(actionInfos);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized void insertActionTable(ActionInfo actionInfo)
      throws MetaStoreException {
    LOG.debug("Insert Action ID {}", actionInfo.getActionId());
    try {
      actionDao.insert(actionInfo);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized void updateActionsTable(ActionInfo[] actionInfos)
      throws MetaStoreException {
    if (actionInfos == null || actionInfos.length == 0) {
      return;
    }
    LOG.debug("Update Action ID {}", actionInfos[0].getActionId());
    try {
      actionDao.update(actionInfos);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<ActionInfo> getNewCreatedActionsTableItem(
      int size) throws MetaStoreException {
    if (size <= 0) {
      return new ArrayList<>();
    }
    try {
      return actionDao.getLatestActions(size);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<ActionInfo> getActionsTableItem(
      List<Long> aids) throws MetaStoreException {
    if (aids == null || aids.size() == 0) {
      return null;
    }
    LOG.debug("Get Action ID {}", aids.toString());
    try {
      return actionDao.getByIds(aids);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<ActionInfo> getActionsTableItem(String aidCondition,
      String cidCondition) throws MetaStoreException {
    LOG.debug("Get aid {} cid {}", aidCondition, cidCondition);
    try {
      return actionDao.getByCondition(aidCondition, cidCondition);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public ActionInfo getActionById(long aid) throws MetaStoreException {
    LOG.debug("Get actioninfo by aid {}", aid);
    try {
      return actionDao.getById(aid);
    } catch (EmptyResultDataAccessException e) {
      return null;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }


  public long getMaxActionId() throws MetaStoreException {
    try {
      return actionDao.getMaxId();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized void insertStoragePolicyTable(StoragePolicy s)
      throws MetaStoreException {
    try {
      storageDao.insertStoragePolicyTable(s);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public String getStoragePolicyName(int sid) throws MetaStoreException {
    updateCache();
    try {
      return mapStoragePolicyIdName.get(sid);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public Integer getStoragePolicyID(
      String policyName) throws MetaStoreException {
    updateCache();
    try {
      return getKey(mapStoragePolicyIdName, policyName);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized boolean insertXattrTable(Long fid, Map<String,
      byte[]> map) throws MetaStoreException {
    try {
      return xattrDao.insertXattrTable(fid, map);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public Map<String, byte[]> getXattrTable(Long fid) throws MetaStoreException {
    try {
      return xattrDao.getXattrTable(fid);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void dropAllTables() throws MetaStoreException {
    Connection conn = getConnection();
    try {
      String url = conn.getMetaData().getURL();
      if (url.startsWith(MetaStoreUtils.SQLITE_URL_PREFIX)) {
        MetaStoreUtils.dropAllTablesSqlite(conn);
      } else if (url.startsWith(MetaStoreUtils.MYSQL_URL_PREFIX)) {
        MetaStoreUtils.dropAllTablesMysql(conn, url);
      } else {
        throw new MetaStoreException("Unsupported database");
      }
    } catch (Exception e) {
      throw new MetaStoreException(e);
    } finally {
      closeConnection(conn);
    }
  }

  public synchronized void initializeDataBase() throws MetaStoreException {
    Connection conn = getConnection();
    try {
      MetaStoreUtils.initializeDataBase(conn);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    } finally {
      closeConnection(conn);
    }
  }

  public synchronized void formatDataBase() throws MetaStoreException {
    dropAllTables();
    initializeDataBase();
  }

  public String aggregateSQLStatement(AccessCountTable destinationTable
      , List<AccessCountTable> tablesToAggregate) throws MetaStoreException {
    try {
      return accessCountDao
          .aggregateSQLStatement(destinationTable, tablesToAggregate);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }
}
