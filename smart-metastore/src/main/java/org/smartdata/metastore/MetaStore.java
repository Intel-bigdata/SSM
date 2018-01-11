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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.metaservice.BackupMetaService;
import org.smartdata.metaservice.CmdletMetaService;
import org.smartdata.metaservice.CopyMetaService;
import org.smartdata.metastore.dao.AccessCountDao;
import org.smartdata.metastore.dao.AccessCountTable;
import org.smartdata.metastore.dao.ActionDao;
import org.smartdata.metastore.dao.BackUpInfoDao;
import org.smartdata.metastore.dao.CacheFileDao;
import org.smartdata.metastore.dao.ClusterConfigDao;
import org.smartdata.metastore.dao.ClusterInfoDao;
import org.smartdata.metastore.dao.CmdletDao;
import org.smartdata.metastore.dao.DataNodeInfoDao;
import org.smartdata.metastore.dao.DataNodeStorageInfoDao;
import org.smartdata.metastore.dao.FileDiffDao;
import org.smartdata.metastore.dao.FileInfoDao;
import org.smartdata.metastore.dao.FileStateDao;
import org.smartdata.metastore.dao.GlobalConfigDao;
import org.smartdata.metastore.dao.GroupsDao;
import org.smartdata.metastore.dao.MetaStoreHelper;
import org.smartdata.metastore.dao.RuleDao;
import org.smartdata.metastore.dao.StorageDao;
import org.smartdata.metastore.dao.StorageHistoryDao;
import org.smartdata.metastore.dao.SystemInfoDao;
import org.smartdata.metastore.dao.UserDao;
import org.smartdata.metastore.dao.XattrDao;
import org.smartdata.metastore.utils.MetaStoreUtils;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.BackUpInfo;
import org.smartdata.model.CachedFileStatus;
import org.smartdata.model.ClusterConfig;
import org.smartdata.model.ClusterInfo;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CmdletState;
import org.smartdata.model.DataNodeInfo;
import org.smartdata.model.DataNodeStorageInfo;
import org.smartdata.model.DetailedFileAction;
import org.smartdata.model.DetailedRuleInfo;
import org.smartdata.model.FileAccessInfo;
import org.smartdata.model.FileDiff;
import org.smartdata.model.FileDiffState;
import org.smartdata.model.FileInfo;
import org.smartdata.model.FileState;
import org.smartdata.model.GlobalConfig;
import org.smartdata.model.NormalFileState;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.RuleState;
import org.smartdata.model.StorageCapacity;
import org.smartdata.model.StoragePolicy;
import org.smartdata.model.SystemInfo;
import org.smartdata.model.XAttribute;
import org.springframework.dao.EmptyResultDataAccessException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.smartdata.metastore.utils.MetaStoreUtils.getKey;

/**
 * Operations supported for upper functions.
 */
public class MetaStore implements CopyMetaService, CmdletMetaService, BackupMetaService {
  static final Logger LOG = LoggerFactory.getLogger(MetaStore.class);

  private DBPool pool = null;

  private Map<Integer, String> mapOwnerIdName = null;
  private Map<Integer, String> mapGroupIdName = null;
  private Map<Integer, String> mapStoragePolicyIdName = null;
  private Map<String, Integer> mapStoragePolicyNameId = null;
  private Map<String, StorageCapacity> mapStorageCapacity = null;
  private Set<String> setBackSrc = null;
  private RuleDao ruleDao;
  private CmdletDao cmdletDao;
  private ActionDao actionDao;
  private FileInfoDao fileInfoDao;
  private CacheFileDao cacheFileDao;
  private StorageDao storageDao;
  private StorageHistoryDao storageHistoryDao;
  private UserDao userDao;
  private GroupsDao groupsDao;
  private XattrDao xattrDao;
  private FileDiffDao fileDiffDao;
  private AccessCountDao accessCountDao;
  private MetaStoreHelper metaStoreHelper;
  private ClusterConfigDao clusterConfigDao;
  private GlobalConfigDao globalConfigDao;
  private DataNodeInfoDao dataNodeInfoDao;
  private DataNodeStorageInfoDao dataNodeStorageInfoDao;
  private BackUpInfoDao backUpInfoDao;
  private ClusterInfoDao clusterInfoDao;
  private SystemInfoDao systemInfoDao;
  private FileStateDao fileStateDao;

  public MetaStore(DBPool pool) throws MetaStoreException {
    this.pool = pool;
    ruleDao = new RuleDao(pool.getDataSource());
    cmdletDao = new CmdletDao(pool.getDataSource());
    actionDao = new ActionDao(pool.getDataSource());
    fileInfoDao = new FileInfoDao(pool.getDataSource());
    xattrDao = new XattrDao(pool.getDataSource());
    cacheFileDao = new CacheFileDao(pool.getDataSource());
    userDao = new UserDao(pool.getDataSource());
    storageDao = new StorageDao(pool.getDataSource());
    storageHistoryDao = new StorageHistoryDao(pool.getDataSource());
    groupsDao = new GroupsDao(pool.getDataSource());
    accessCountDao = new AccessCountDao(pool.getDataSource());
    fileDiffDao = new FileDiffDao(pool.getDataSource());
    metaStoreHelper = new MetaStoreHelper(pool.getDataSource());
    clusterConfigDao = new ClusterConfigDao(pool.getDataSource());
    globalConfigDao = new GlobalConfigDao(pool.getDataSource());
    dataNodeInfoDao = new DataNodeInfoDao(pool.getDataSource());
    dataNodeStorageInfoDao = new DataNodeStorageInfoDao(pool.getDataSource());
    backUpInfoDao = new BackUpInfoDao(pool.getDataSource());
    clusterInfoDao = new ClusterInfoDao(pool.getDataSource());
    systemInfoDao = new SystemInfoDao(pool.getDataSource());
    fileStateDao = new FileStateDao(pool.getDataSource());
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
    fileInfoDao.updateUsersMap(mapOwnerIdName);
  }

  private void updateGroupsMap() throws MetaStoreException {
    try {
      mapGroupIdName = groupsDao.getGroupsMap();
      fileInfoDao.updateGroupsMap(mapGroupIdName);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  /**
   * Store a single file info into database.
   *
   * @param file
   */
  public synchronized void insertFile(FileInfo file)
      throws MetaStoreException {
    updateCache();
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
    fileInfoDao.insert(file);
  }


  /**
   * Store files info into database.
   *
   * @param files
   */
  public synchronized void insertFiles(FileInfo[] files)
      throws MetaStoreException {
    updateCache();
    for (FileInfo file : files) {
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

  public FileInfo getFile(long fid) throws MetaStoreException {
    updateCache();
    try {
      return fileInfoDao.getById(fid);
    } catch (EmptyResultDataAccessException e) {
      return null;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public FileInfo getFile(String path) throws MetaStoreException {
    updateCache();
    try {
      return fileInfoDao.getByPath(path);
    } catch (EmptyResultDataAccessException e) {
      return null;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<FileInfo> getFile() throws MetaStoreException {
    updateCache();
    try {
      return fileInfoDao.getAll();
    } catch (EmptyResultDataAccessException e) {
      return new ArrayList<>();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<FileInfo> getFilesByPrefix(String path) throws MetaStoreException {
    updateCache();
    try {
      return fileInfoDao.getFilesByPrefix(path);
    } catch (EmptyResultDataAccessException e) {
      return new ArrayList<>();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public Map<String, Long> getFileIDs(Collection<String> paths)
      throws MetaStoreException {
    try {
      return fileInfoDao.getPathFids(paths);
    } catch (EmptyResultDataAccessException e) {
      return new HashMap<>();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public Map<Long, String> getFilePaths(Collection<Long> ids)
      throws MetaStoreException {
    try {
      return fileInfoDao.getFidPaths(ids);
    } catch (EmptyResultDataAccessException e) {
      return new HashMap<>();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized List<FileAccessInfo> getHotFiles(
      List<AccessCountTable> tables,
      int topNum) throws MetaStoreException {
    Iterator<AccessCountTable> tableIterator = tables.iterator();
    if (tableIterator.hasNext()) {
      try {
        Map<Long, Integer> accessCounts =
            accessCountDao.getHotFiles(tables, topNum);
        if (accessCounts.size() == 0) {
          return new ArrayList<>();
        }
        Map<Long, String> idToPath = getFilePaths(accessCounts.keySet());
        List<FileAccessInfo> result = new ArrayList<>();
        for (Map.Entry<Long, Integer> entry : accessCounts.entrySet()) {
          Long fid = entry.getKey();
          if (idToPath.containsKey(fid) && entry.getValue() > 0) {
            result.add(
                new FileAccessInfo(fid, idToPath.get(fid), entry.getValue()));
          }
        }
        return result;
      } catch (EmptyResultDataAccessException e) {
        return new ArrayList<>();
      } catch (Exception e) {
        throw new MetaStoreException(e);
      } finally {
        for (AccessCountTable accessCountTable : tables) {
          if (accessCountTable.isEphemeral()) {
            this.dropTable(accessCountTable.getTableName());
          }
        }
      }
    } else {
      return new ArrayList<>();
    }
  }

  public void deleteAllFileInfo() throws MetaStoreException {
    try {
      fileInfoDao.deleteAll();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<AccessCountTable> getAllSortedTables() throws MetaStoreException {
    try {
      return accessCountDao.getAllSortedTables();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void deleteAccessCountTable(
      AccessCountTable table) throws MetaStoreException {
    try {
      accessCountDao.delete(table);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void insertAccessCountTable(
      AccessCountTable accessCountTable) throws MetaStoreException {
    try {
      accessCountDao.insert(accessCountTable);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void insertUpdateStoragesTable(StorageCapacity[] storages)
      throws MetaStoreException {
    mapStorageCapacity = null;
    try {
      storageDao.insertUpdateStoragesTable(storages);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void insertUpdateStoragesTable(List<StorageCapacity> storages)
      throws MetaStoreException {
    mapStorageCapacity = null;
    try {
      storageDao.insertUpdateStoragesTable(
          storages.toArray(new StorageCapacity[storages.size()]));
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void insertUpdateStoragesTable(StorageCapacity storage)
      throws MetaStoreException {
    insertUpdateStoragesTable(new StorageCapacity[]{storage});
  }

  public Map<String, StorageCapacity> getStorageCapacity() throws MetaStoreException {
    if (mapStorageCapacity == null) {
      updateCache();
    }

    Map<String, StorageCapacity> ret = new HashMap<>();
    if (mapStorageCapacity != null) {
      for (String key : mapStorageCapacity.keySet()) {
        ret.put(key, mapStorageCapacity.get(key));
      }
    }
    return ret;
  }

  public void deleteStorage(String storageType) throws MetaStoreException {
    try {
      storageDao.deleteStorage(storageType);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public StorageCapacity getStorageCapacity(
      String type) throws MetaStoreException {
    updateCache();
    try {
      return mapStorageCapacity.get(type);
    } catch (EmptyResultDataAccessException e) {
      return null;
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

  public synchronized void insertStorageHistTable(StorageCapacity[] storages, long interval)
      throws MetaStoreException {
    try {
      storageHistoryDao.insertStorageHistTable(storages, interval);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<StorageCapacity> getStorageHistoryData(String type, long interval,
      long startTime, long endTime) {
    return storageHistoryDao.getStorageHistoryData(type, interval, startTime, endTime);
  }

  public void deleteStorageHistoryOldRecords(String type, long interval, long beforTimeStamp)
      throws MetaStoreException {
    try {
      storageHistoryDao.deleteOldRecords(type, interval, beforTimeStamp);
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
    } catch (EmptyResultDataAccessException e) {
      return new ArrayList<>();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<Long> getCachedFids() throws MetaStoreException {
    try {
      return cacheFileDao.getFids();
    } catch (EmptyResultDataAccessException e) {
      return new ArrayList<>();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public CachedFileStatus getCachedFileStatus(
      long fid) throws MetaStoreException {
    try {
      return cacheFileDao.getById(fid);
    } catch (EmptyResultDataAccessException e) {
      return null;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void createProportionTable(AccessCountTable dest,
      AccessCountTable source)
      throws MetaStoreException {
    try {
      accessCountDao.createProportionTable(dest, source);
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
      LOG.debug("ExecuteFilesPathQuer sql = {}", sql);
      return metaStoreHelper.getFilesPath(sql);
    } catch (EmptyResultDataAccessException e) {
      return new ArrayList<>();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<DetailedFileAction> listFileActions(long rid, int size) throws MetaStoreException {
    if (mapStoragePolicyIdName == null) {
      updateCache();
    }
    List<ActionInfo> actionInfos = getActions(rid, size);
    List<DetailedFileAction> detailedFileActions = new ArrayList<>();

    for (ActionInfo actionInfo : actionInfos) {
      DetailedFileAction detailedFileAction = new DetailedFileAction(actionInfo);
      String filePath = actionInfo.getArgs().get("-file");
      FileInfo fileInfo = getFile(filePath);
      if (fileInfo == null) {
        // LOG.debug("Namespace is not sync! File {} not in file table!", filePath);
        // Add a mock fileInfo
        fileInfo = new FileInfo(filePath, 0L, 0L, false,
            (short) 0, 0L, 0L, 0L, (short) 0,
            "root", "root", (byte) 0);
      }
      detailedFileAction.setFileLength(fileInfo.getLength());
      detailedFileAction.setFilePath(filePath);
      if (actionInfo.getActionName().contains("allssd")
          || actionInfo.getActionName().contains("onessd")
          || actionInfo.getActionName().contains("archive")) {
        detailedFileAction.setTarget(actionInfo.getActionName());
        detailedFileAction.setSrc(mapStoragePolicyIdName.get((int) fileInfo.getStoragePolicy()));
      } else {
        detailedFileAction.setSrc(actionInfo.getArgs().get("-src"));
        detailedFileAction.setTarget(actionInfo.getArgs().get("-dest"));
      }
      detailedFileActions.add(detailedFileAction);
    }
    return detailedFileActions;
  }

  public List<DetailedFileAction> listFileActions(long rid, long start, long offset)
      throws MetaStoreException {
    if (mapStoragePolicyIdName == null) {
      updateCache();
    }
    List<ActionInfo> actionInfos = getActions(rid, start, offset);
    List<DetailedFileAction> detailedFileActions = new ArrayList<>();
    for (ActionInfo actionInfo : actionInfos) {
      DetailedFileAction detailedFileAction = new DetailedFileAction(actionInfo);
      String filePath = actionInfo.getArgs().get("-file");
      FileInfo fileInfo = getFile(filePath);
      if (fileInfo == null) {
        // LOG.debug("Namespace is not sync! File {} not in file table!", filePath);
        // Add a mock fileInfo
        fileInfo = new FileInfo(filePath, 0L, 0L, false,
            (short) 0, 0L, 0L, 0L, (short) 0,
            "root", "root", (byte) 0);
      }
      detailedFileAction.setFileLength(fileInfo.getLength());
      detailedFileAction.setFilePath(filePath);
      if (actionInfo.getActionName().contains("allssd")
          || actionInfo.getActionName().contains("onessd")
          || actionInfo.getActionName().contains("archive")) {
        detailedFileAction.setTarget(actionInfo.getActionName());
        detailedFileAction.setSrc(mapStoragePolicyIdName.get((int) fileInfo.getStoragePolicy()));
      } else {
        detailedFileAction.setSrc(actionInfo.getArgs().get("-src"));
        detailedFileAction.setTarget(actionInfo.getArgs().get("-dest"));
      }
      detailedFileActions.add(detailedFileAction);
    }
    return detailedFileActions;
  }

  public long getNumFileAction(long rid) throws MetaStoreException {
    return listFileActions(rid, 0).size();
  }

  public List<DetailedRuleInfo> listMoveRules() throws MetaStoreException {
    List<RuleInfo> ruleInfos = getRuleInfo();
    List<DetailedRuleInfo> detailedRuleInfos = new ArrayList<>();
    for (RuleInfo ruleInfo : ruleInfos) {
      if (ruleInfo.getRuleText().contains("allssd")
          || ruleInfo.getRuleText().contains("onessd")
          || ruleInfo.getRuleText().contains("archive")) {
        DetailedRuleInfo detailedRuleInfo = new DetailedRuleInfo(ruleInfo);
        // Add mover progress
        List<CmdletInfo> cmdletInfos = cmdletDao.getByRid(ruleInfo.getId());
        int currPos = 0;
        for (CmdletInfo cmdletInfo : cmdletInfos) {
          if (cmdletInfo.getState().getValue() <= 2) {
            break;
          }
          currPos += 1;
        }
        int countRunning = 0;
        for (CmdletInfo cmdletInfo : cmdletInfos) {
          if (cmdletInfo.getState().getValue() <= 2) {
            countRunning += 1;
          }
        }
        detailedRuleInfo
            .setBaseProgress(cmdletInfos.size() - currPos);
        detailedRuleInfo.setRunningProgress(countRunning);
        detailedRuleInfos.add(detailedRuleInfo);
      }
    }
    return detailedRuleInfos;
  }


  public List<DetailedRuleInfo> listSyncRules() throws MetaStoreException {
    List<RuleInfo> ruleInfos = getRuleInfo();
    List<DetailedRuleInfo> detailedRuleInfos = new ArrayList<>();
    for (RuleInfo ruleInfo : ruleInfos) {
      if (ruleInfo.getState() == RuleState.DELETED) {
        continue;
      }
      if (ruleInfo.getRuleText().contains("sync")) {
        DetailedRuleInfo detailedRuleInfo = new DetailedRuleInfo(ruleInfo);
        // Add sync progress
        BackUpInfo backUpInfo = getBackUpInfo(ruleInfo.getId());
        // Get total matched files
        if (backUpInfo != null) {
          detailedRuleInfo
              .setBaseProgress(getFilesByPrefix(backUpInfo.getSrc()).size());
          int count = fileDiffDao.getPendingDiff(backUpInfo.getSrc()).size();
          count += fileDiffDao.getByState(backUpInfo.getSrc(), FileDiffState.RUNNING).size();
          detailedRuleInfo.setRunningProgress(count);
        } else {
          detailedRuleInfo
              .setBaseProgress(0);
          detailedRuleInfo.setRunningProgress(0);
        }
        detailedRuleInfos.add(detailedRuleInfo);
      }
    }
    return detailedRuleInfos;
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

  public synchronized boolean updateRuleState(long ruleId, RuleState rs)
      throws MetaStoreException {
    if (rs == null) {
      throw new MetaStoreException("Rule state can not be null, ruleId = " + ruleId);
    }
    try {
      return ruleDao.update(ruleId, rs.getValue()) >= 0;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public RuleInfo getRuleInfo(long ruleId) throws MetaStoreException {
    try {
      return ruleDao.getById(ruleId);
    } catch (EmptyResultDataAccessException e) {
      return null;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<RuleInfo> listPageRule(long start, long offset, List<String> orderBy,
      List<Boolean> desc)
      throws MetaStoreException {
    LOG.debug("List Rule, start {}, offset {}", start, offset);
    try {
      if (orderBy.size() == 0) {
        return ruleDao.getAPageOfRule(start, offset);
      } else {
        return ruleDao.getAPageOfRule(start, offset, orderBy, desc);
      }
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }


  public List<RuleInfo> getRuleInfo() throws MetaStoreException {
    try {
      return ruleDao.getAll();
    } catch (EmptyResultDataAccessException e) {
      return new ArrayList<>();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<CmdletInfo> listPageCmdlets(long rid, long start, long offset,
      List<String> orderBy, List<Boolean> desc)
      throws MetaStoreException {
    LOG.debug("List cmdlet, start {}, offset {}", start, offset);
    try {
      if (orderBy.size() == 0) {
        return cmdletDao.getByRid(rid, start, offset);
      } else {
        return cmdletDao.getByRid(rid, start, offset, orderBy, desc);
      }
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public long getNumCmdletsByRid(long rid) {
    try {
        return cmdletDao.getNumByRid(rid);
    } catch (Exception e) {
      return 0;
    }
  }

  public List<CmdletInfo> listPageCmdlets(long start, long offset,
      List<String> orderBy, List<Boolean> desc)
      throws MetaStoreException {
    LOG.debug("List cmdlet, start {}, offset {}", start, offset);
    try {
      if (orderBy.size() == 0) {
        return cmdletDao.getAPageOfCmdlet(start, offset);
      } else {
        return cmdletDao.getAPageOfCmdlet(start, offset, orderBy, desc);
      }
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void deleteAllRules() throws MetaStoreException {
    try {
      ruleDao.deleteAll();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }


  public synchronized void insertCmdlets(CmdletInfo[] commands)
      throws MetaStoreException {
    try {
      cmdletDao.insert(commands);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized void insertCmdlet(CmdletInfo command)
      throws MetaStoreException {
    try {
      // Update if exists
      if (getCmdletById(command.getCid()) != null) {
        cmdletDao.update(command);
      } else {
        cmdletDao.insert(command);
      }
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

  public List<CmdletInfo> getCmdlets(String cidCondition,
      String ridCondition, CmdletState state) throws MetaStoreException {
    try {
      return cmdletDao.getByCondition(cidCondition, ridCondition, state);
    } catch (EmptyResultDataAccessException e) {
      return new ArrayList<>();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<CmdletInfo> getCmdlets(CmdletState state) throws MetaStoreException {
    try {
      return cmdletDao.getByState(state);
    } catch (EmptyResultDataAccessException e) {
      return new ArrayList<>();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public boolean updateCmdlet(CmdletInfo cmdletInfo)
      throws MetaStoreException {
    try {
      return cmdletDao.update(cmdletInfo) >= 0;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public boolean updateCmdlet(long cid, long rid, CmdletState state)
      throws MetaStoreException {
    try {
      return cmdletDao.update(cid, rid, state.getValue()) >= 0;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public boolean updateCmdlet(long cid, String parameters, CmdletState state)
      throws MetaStoreException {
    try {
      return cmdletDao.update(cid, parameters, state.getValue()) >= 0;
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

  /**
   * Delete finished cmdlets before given timestamp, actions belonging to these cmdlets
   * will also be deleted. Cmdlet's generate_time is used for comparison.
   *
   * @param timestamp
   * @return number of cmdlets deleted
   * @throws MetaStoreException
   */
  public int deleteFinishedCmdletsWithGenTimeBefore(long timestamp) throws MetaStoreException {
    try {
      return cmdletDao.deleteBeforeTime(timestamp);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public int deleteKeepNewCmdlets(long num) throws MetaStoreException {
    try {
      return cmdletDao.deleteKeepNewCmd(num);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public int getNumCmdletsInTerminiatedStates() throws MetaStoreException {
    try {
      return cmdletDao.getNumCmdletsInTerminiatedStates();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized void insertActions(ActionInfo[] actionInfos)
      throws MetaStoreException {
    try {
      actionDao.insert(actionInfos);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized void insertAction(ActionInfo actionInfo)
      throws MetaStoreException {
    LOG.debug("Insert Action ID {}", actionInfo.getActionId());
    try {
      actionDao.insert(actionInfo);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<ActionInfo> listPageAction(long start, long offset, List<String> orderBy,
      List<Boolean> desc)
      throws MetaStoreException {
    LOG.debug("List Action, start {}, offset {}", start, offset);
    try {
      if (orderBy.size() == 0) {
        return actionDao.getAPageOfAction(start, offset);
      } else {
        return actionDao.getAPageOfAction(start, offset, orderBy, desc);
      }
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void deleteCmdletActions(long cmdletId) throws MetaStoreException {
    try {
      actionDao.deleteCmdletActions(cmdletId);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void deleteAllActions() throws MetaStoreException {
    try {
      actionDao.deleteAll();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  /**
   * Mark action {aid} as failed.
   *
   * @param aid
   * @throws MetaStoreException
   */
  public void markActionFailed(long aid) throws MetaStoreException {
    ActionInfo actionInfo = getActionById(aid);
    if (actionInfo != null) {
      // Finished
      actionInfo.setFinished(true);
      // Failed
      actionInfo.setSuccessful(false);
      // 100 % progress
      actionInfo.setProgress(1);
      // Finish time equals to create time
      actionInfo.setFinishTime(actionInfo.getCreateTime());
      updateAction(actionInfo);
    }
  }

  public void updateAction(ActionInfo actionInfo) throws MetaStoreException {
    if (actionInfo == null) {
      return;
    }
    LOG.debug("Update Action ID {}", actionInfo.getActionId());
    try {
      actionDao.update(actionInfo);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized void updateActions(ActionInfo[] actionInfos)
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

  public List<ActionInfo> getNewCreatedActions(
      int size) throws MetaStoreException {
    if (size < 0) {
      return new ArrayList<>();
    }
    try {
      return actionDao.getLatestActions(size);
    } catch (EmptyResultDataAccessException e) {
      return new ArrayList<>();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<ActionInfo> getNewCreatedActions(String actionName,
      int size) throws MetaStoreException {
    if (size < 0) {
      return new ArrayList<>();
    }
    try {
      return actionDao.getLatestActions(actionName, size);
    } catch (EmptyResultDataAccessException e) {
      return new ArrayList<>();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<ActionInfo> getNewCreatedActions(String actionName,
      int size, boolean successful,
      boolean finished) throws MetaStoreException {
    if (size < 0) {
      return new ArrayList<>();
    }
    try {
      return actionDao.getLatestActions(actionName, size, successful, finished);
    } catch (EmptyResultDataAccessException e) {
      return new ArrayList<>();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<ActionInfo> getNewCreatedActions(String actionName,
      int size, boolean finished) throws MetaStoreException {
    if (size < 0) {
      return new ArrayList<>();
    }
    try {
      return actionDao.getLatestActions(actionName, size, finished);
    } catch (EmptyResultDataAccessException e) {
      return new ArrayList<>();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<ActionInfo> getNewCreatedActions(String actionName,
      boolean successful, int size) throws MetaStoreException {
    if (size < 0) {
      return new ArrayList<>();
    }
    try {
      return actionDao.getLatestActions(actionName, size, successful);
    } catch (EmptyResultDataAccessException e) {
      return new ArrayList<>();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }


  public List<ActionInfo> getActions(
      List<Long> aids) throws MetaStoreException {
    if (aids == null || aids.size() == 0) {
      return new ArrayList<>();
    }
    LOG.debug("Get Action ID {}", aids.toString());
    try {
      return actionDao.getByIds(aids);
    } catch (EmptyResultDataAccessException e) {
      return new ArrayList<>();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<ActionInfo> getActions(String aidCondition,
      String cidCondition) throws MetaStoreException {
    LOG.debug("Get aid {} cid {}", aidCondition, cidCondition);
    try {
      return actionDao.getByCondition(aidCondition, cidCondition);
    } catch (EmptyResultDataAccessException e) {
      return new ArrayList<>();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<ActionInfo> getActions(long rid, int size) throws MetaStoreException {
    if (size <= 0) {
      size = Integer.MAX_VALUE;
    }
    List<CmdletInfo> cmdletInfos = cmdletDao.getByRid(rid);
    List<ActionInfo> runningActions = new ArrayList<>();
    List<ActionInfo> finishedActions = new ArrayList<>();
    int total = 0;
    for (CmdletInfo cmdletInfo : cmdletInfos) {
      if (total >= size) {
        break;
      }
      List<Long> aids = cmdletInfo.getAids();
      for (Long aid : aids) {
        if (total >= size) {
          break;
        }
        ActionInfo actionInfo = getActionById(aid);
        if (actionInfo.isFinished()) {
          finishedActions.add(actionInfo);
        } else {
          runningActions.add(actionInfo);
        }
        total++;
      }
    }
    runningActions.addAll(finishedActions);
    return runningActions;
  }

  public List<ActionInfo> getActions(long rid, long start, long offset) throws MetaStoreException {
    long mark = 0;
    long count = 0;
    List<CmdletInfo> cmdletInfos = cmdletDao.getByRid(rid);
    List<ActionInfo> totalActions = new ArrayList<>();
    for (CmdletInfo cmdletInfo : cmdletInfos) {
      List<Long> aids = cmdletInfo.getAids();
      if (mark + aids.size() >= start + 1) {
        long gap;
        gap = start - mark;
        for (Long aid : aids) {
          if (gap > 0) {
            gap--;
            mark++;
            continue;
          }
          if (count < offset) {
            ActionInfo actionInfo = getActionById(aid);
            totalActions.add(actionInfo);
            count++;
            mark++;
          } else {
            return totalActions;
          }
        }
      } else {
        mark += aids.size();
      }

    }
    return totalActions;
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

  public long getCountOfAllAction() throws MetaStoreException {
    try {
      return actionDao.getCountOfAction();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized void insertStoragePolicy(StoragePolicy s)
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
    } catch (EmptyResultDataAccessException e) {
      return null;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public Integer getStoragePolicyID(
      String policyName) throws MetaStoreException {
    updateCache();
    try {
      return getKey(mapStoragePolicyIdName, policyName);
    } catch (EmptyResultDataAccessException e) {
      return -1;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized boolean insertXattrList(Long fid,
      List<XAttribute> attributes) throws MetaStoreException {
    try {
      return xattrDao.insertXattrList(fid, attributes);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<XAttribute> getXattrList(Long fid) throws MetaStoreException {
    try {
      return xattrDao.getXattrList(fid);
    } catch (EmptyResultDataAccessException e) {
      return new ArrayList<>();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  @Override
  public synchronized boolean insertFileDiff(FileDiff fileDiff)
      throws MetaStoreException {
    try {
      return fileDiffDao.insert(fileDiff) >= 0;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized void insertFileDiffs(FileDiff[] fileDiffs)
      throws MetaStoreException {
    try {
      fileDiffDao.insert(fileDiffs);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public synchronized void insertFileDiffs(List<FileDiff> fileDiffs)
      throws MetaStoreException {
    try {
      fileDiffDao.insert(fileDiffs);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public FileDiff getFileDiff(long did) throws MetaStoreException {
    try {
      return fileDiffDao.getById(did);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<FileDiff> getFileDiffsByFileName(String fileName) throws MetaStoreException {
    try {
      return fileDiffDao.getByFileName(fileName);
    } catch (EmptyResultDataAccessException e) {
      return new ArrayList<>();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<FileDiff> getFileDiffs(FileDiffState fileDiffState) throws MetaStoreException {
    try {
      return fileDiffDao.getByState(fileDiffState);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  @Override
  public boolean updateFileDiff(long did,
      FileDiffState state) throws MetaStoreException {
    try {
      return fileDiffDao.update(did, state) >= 0;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public boolean batchUpdateFileDiff(
      List<Long> did, List<FileDiffState> states, List<String> parameters)
      throws MetaStoreException {
    try {
      return fileDiffDao.batchUpdate(did, states, parameters).length > 0;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public boolean updateFileDiff(long did,
      FileDiffState state, String parameters) throws MetaStoreException {
    try {
      return fileDiffDao.update(did, state, parameters) >= 0;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public boolean updateFileDiff(long did,
      String src) throws MetaStoreException {
    try {
      return fileDiffDao.update(did, src) >= 0;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public boolean updateFileDiff(FileDiff fileDiff)
      throws MetaStoreException {
    long did = fileDiff.getDiffId();
    FileDiff preFileDiff = getFileDiff(did);
    if (preFileDiff == null) {
      insertFileDiff(fileDiff);
    }
    try {
      return fileDiffDao.update(fileDiff) >= 0;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public boolean updateFileDiff(List<FileDiff> fileDiffs)
    throws MetaStoreException {
    if (fileDiffs == null || fileDiffs.size() == 0) {
      return true;
    }
    for (FileDiff fileDiff: fileDiffs) {
      if (!updateFileDiff(fileDiff)) {
        return false;
      }
    }
    return true;
  }


  public List<String> getSyncPath(int size) throws MetaStoreException {
    return fileDiffDao.getSyncPath(size);
  }

  @Override
  public List<FileDiff> getPendingDiff() throws MetaStoreException {
    return fileDiffDao.getPendingDiff();
  }

  @Override
  public List<FileDiff> getPendingDiff(long rid) throws MetaStoreException {
    return fileDiffDao.getPendingDiff(rid);
  }

  @Override
  public void deleteAllFileDiff() throws MetaStoreException {
    fileDiffDao.deleteAll();
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

  public synchronized void checkTables() throws MetaStoreException {
    Connection conn = getConnection();
    try {
      if (!MetaStoreUtils.isTableSetExist(conn)) {
        LOG.info("At least one table required by SSM is missing. "
                + "The configured database will be formatted.");
        formatDataBase();
      }
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

  public void aggregateTables(AccessCountTable destinationTable
      , List<AccessCountTable> tablesToAggregate) throws MetaStoreException {
    try {
      accessCountDao.aggregateTables(destinationTable, tablesToAggregate);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void setClusterConfig(
      ClusterConfig clusterConfig) throws MetaStoreException {
    try {

      if (clusterConfigDao.getCountByName(clusterConfig.getNodeName()) == 0) {
        //insert
        clusterConfigDao.insert(clusterConfig);
      } else {
        //update
        clusterConfigDao.updateByNodeName(clusterConfig.getNodeName(),
            clusterConfig.getConfigPath());
      }
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void delClusterConfig(
      ClusterConfig clusterConfig) throws MetaStoreException {
    try {
      if (clusterConfigDao.getCountByName(clusterConfig.getNodeName()) > 0) {
        //insert
        clusterConfigDao.delete(clusterConfig.getCid());
      }
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<ClusterConfig> listClusterConfig() throws MetaStoreException {
    try {
      return clusterConfigDao.getAll();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public GlobalConfig getDefaultGlobalConfigByName(
      String configName) throws MetaStoreException {
    try {
      if (globalConfigDao.getCountByName(configName) > 0) {
        //the property is existed
        return globalConfigDao.getByPropertyName(configName);
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void setGlobalConfig(
      GlobalConfig globalConfig) throws MetaStoreException {
    try {
      if (globalConfigDao.getCountByName(globalConfig.getPropertyName()) > 0) {
        globalConfigDao.update(globalConfig.getPropertyName(),
            globalConfig.getPropertyValue());
      } else {
        globalConfigDao.insert(globalConfig);
      }
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void insertDataNodeInfo(DataNodeInfo dataNodeInfo)
      throws MetaStoreException {
    try {
      dataNodeInfoDao.insert(dataNodeInfo);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void insertDataNodeInfos(DataNodeInfo[] dataNodeInfos)
      throws MetaStoreException {
    try {
      dataNodeInfoDao.insert(dataNodeInfos);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void insertDataNodeInfos(List<DataNodeInfo> dataNodeInfos)
      throws MetaStoreException {
    try {
      dataNodeInfoDao.insert(dataNodeInfos);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<DataNodeInfo> getDataNodeInfoByUuid(String uuid)
      throws MetaStoreException {
    try {
      return dataNodeInfoDao.getByUuid(uuid);
    } catch (EmptyResultDataAccessException e) {
      return null;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<DataNodeInfo> getAllDataNodeInfo()
      throws MetaStoreException {
    try {
      return dataNodeInfoDao.getAll();
    } catch (EmptyResultDataAccessException e) {
      return null;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void deleteDataNodeInfo(String uuid)
      throws MetaStoreException {
    try {
      dataNodeInfoDao.delete(uuid);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void deleteAllDataNodeInfo()
      throws MetaStoreException {
    try {
      dataNodeInfoDao.deleteAll();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void insertDataNodeStorageInfo(DataNodeStorageInfo dataNodeStorageInfo)
      throws MetaStoreException {
    try {
      dataNodeStorageInfoDao.insert(dataNodeStorageInfo);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void insertDataNodeStorageInfos(
      DataNodeStorageInfo[] dataNodeStorageInfos)
      throws MetaStoreException {
    try {
      dataNodeStorageInfoDao.insert(dataNodeStorageInfos);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void insertDataNodeStorageInfos(
      List<DataNodeStorageInfo> dataNodeStorageInfos)
      throws MetaStoreException {
    try {
      dataNodeStorageInfoDao.insert(dataNodeStorageInfos);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public boolean judgeTheRecordIfExist(String storageType) throws MetaStoreException {
    try {
      if (storageDao.getCountOfStorageType(storageType) < 1) {
        return false;
      } else {
        return true;
      }
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  //need to be triggered when DataNodeStorageInfo table is changed
  public long getStoreCapacityOfDifferentStorageType(String storageType) throws MetaStoreException {
    try {
      int sid = 0;

      if (storageType.equals("ram")) {
        sid = 0;
      }

      if (storageType.equals("ssd")) {
        sid = 1;
      }

      if (storageType.equals("disk")) {
        sid = 2;
      }

      if (storageType.equals("archive")) {
        sid = 3;
      }
      List<DataNodeStorageInfo> lists = dataNodeStorageInfoDao.getBySid(sid);
      long allCapacity = 0;
      for (DataNodeStorageInfo info : lists) {
        allCapacity = allCapacity + info.getCapacity();
      }
      return allCapacity;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  //need to be triggered when DataNodeStorageInfo table is changed
  public long getStoreFreeOfDifferentStorageType(String storageType) throws MetaStoreException {
    try {
      int sid = 0;

      if (storageType.equals("ram")) {
        sid = 0;
      }

      if (storageType.equals("ssd")) {
        sid = 1;
      }

      if (storageType.equals("disk")) {
        sid = 2;
      }

      if (storageType.equals("archive")) {
        sid = 3;
      }
      List<DataNodeStorageInfo> lists = dataNodeStorageInfoDao.getBySid(sid);
      long allFree = 0;
      for (DataNodeStorageInfo info : lists) {
        allFree = allFree + info.getRemaining();
      }
      return allFree;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<DataNodeStorageInfo> getDataNodeStorageInfoByUuid(String uuid)
      throws MetaStoreException {
    try {
      return dataNodeStorageInfoDao.getByUuid(uuid);
    } catch (EmptyResultDataAccessException e) {
      return null;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }


  public List<DataNodeStorageInfo> getAllDataNodeStorageInfo()
      throws MetaStoreException {
    try {
      return dataNodeStorageInfoDao.getAll();
    } catch (EmptyResultDataAccessException e) {
      return null;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void deleteDataNodeStorageInfo(String uuid)
      throws MetaStoreException {
    try {
      dataNodeStorageInfoDao.delete(uuid);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void deleteAllDataNodeStorageInfo()
      throws MetaStoreException {
    try {
      dataNodeStorageInfoDao.deleteAll();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<BackUpInfo> listAllBackUpInfo() throws MetaStoreException {
    try {
      return backUpInfoDao.getAll();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public boolean srcInbackup(String src) throws MetaStoreException {
    if (setBackSrc == null) {
      setBackSrc = new HashSet<>();
      List<BackUpInfo> backUpInfos = listAllBackUpInfo();
      for (BackUpInfo backUpInfo : backUpInfos) {
        setBackSrc.add(backUpInfo.getSrc());
      }
    }
    // LOG.info("Backup src = {}, setBackSrc {}", src, setBackSrc);
    for (String srcDir : setBackSrc) {
      if (src.startsWith(srcDir)) {
        return true;
      }
    }
    return false;
  }

  public BackUpInfo getBackUpInfo(long rid) throws MetaStoreException {
    try {
      return backUpInfoDao.getByRid(rid);
    } catch (EmptyResultDataAccessException e) {
      return null;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void deleteAllBackUpInfo() throws MetaStoreException {
    try {
      backUpInfoDao.deleteAll();
      setBackSrc.clear();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void deleteBackUpInfo(long rid) throws MetaStoreException {
    try {
      BackUpInfo backUpInfo = getBackUpInfo(rid);
      if (backUpInfo != null) {
        if (backUpInfoDao.getBySrc(backUpInfo.getSrc()).size() == 1) {
          if (setBackSrc != null) {
            setBackSrc.remove(backUpInfo.getSrc());
          }
        }
        backUpInfoDao.delete(rid);
      }
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void insertBackUpInfo(
      BackUpInfo backUpInfo) throws MetaStoreException {
    try {
      backUpInfoDao.insert(backUpInfo);
      if (setBackSrc == null) {
        setBackSrc = new HashSet<>();
      }
      setBackSrc.add(backUpInfo.getSrc());
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<ClusterInfo> listAllClusterInfo() throws MetaStoreException {
    try {
      return clusterInfoDao.getAll();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public List<SystemInfo> listAllSystemInfo() throws MetaStoreException {
    try {
      return systemInfoDao.getAll();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }


  public ClusterInfo getClusterInfoByCid(long id) throws MetaStoreException {
    try {
      return clusterInfoDao.getById(id);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public SystemInfo getSystemInfoByProperty(
      String property) throws MetaStoreException {
    try {
      return systemInfoDao.getByProperty(property);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public boolean containSystemInfo(String property) throws MetaStoreException {
    try {
      return systemInfoDao.containsProperty(property);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void deleteAllClusterInfo() throws MetaStoreException {
    try {
      clusterInfoDao.deleteAll();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void updateSystemInfo(
      SystemInfo systemInfo) throws MetaStoreException {
    try {
      systemInfoDao.update(systemInfo);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void updateAndInsertIfNotExist(
      SystemInfo systemInfo) throws MetaStoreException {
    try {
      if (systemInfoDao.update(systemInfo) <= 0) {
        systemInfoDao.insert(systemInfo);
      }
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void deleteClusterInfo(long cid) throws MetaStoreException {
    try {
      clusterInfoDao.delete(cid);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void deleteSystemInfo(
      String property) throws MetaStoreException {
    try {
      systemInfoDao.delete(property);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void insertClusterInfo(
      ClusterInfo clusterInfo) throws MetaStoreException {
    try {
      if (clusterInfoDao.getCountByName(clusterInfo.getName()) != 0) {
        throw new Exception("name has already exist");
      }
      clusterInfoDao.insert(clusterInfo);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void insertSystemInfo(
      SystemInfo systemInfo) throws MetaStoreException {
    try {
      if (systemInfoDao.containsProperty(systemInfo.getProperty())) {
        throw new Exception("The system property already exists");
      }
      systemInfoDao.insert(systemInfo);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public void insertUpdateFileState(FileState fileState) {
    fileStateDao.insertUpate(fileState);
    // Update corresponding table if fileState is a specific FileState
    /*
    if (fileState instanceof CompressFileState) {

    } else if (fileState instanceof CompactFileState) {

    } else if (fileState instanceof S3FileState) {

    }
    */
  }

  public FileState getFileState(String path) {
    FileState fileState;
    try {
      fileState = fileStateDao.getByPath(path);
    } catch (EmptyResultDataAccessException e) {
      fileState = new NormalFileState(path);
    }

    // Fetch info from corresponding table to regenerate a specific FileState
    switch (fileState.getFileType()) {
      case NORMAL:
        fileState = new NormalFileState(path);
        break;
      case COMPACT:
        break;
      case COMPRESSION:
        break;
      case S3:
        break;
      default:
    }
    return fileState;
  }
}
