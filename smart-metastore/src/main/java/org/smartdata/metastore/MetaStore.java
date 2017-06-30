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


import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.common.CmdletState;
import org.smartdata.common.models.ActionInfo;
import org.smartdata.common.models.CmdletInfo;
import org.smartdata.common.CachedFileStatus;
import org.smartdata.common.models.FileAccessInfo;
import org.smartdata.common.models.FileInfo;
import org.smartdata.common.models.FileStatusInternal;
import org.smartdata.common.models.RuleInfo;
import org.smartdata.common.models.StorageCapacity;
import org.smartdata.common.models.StoragePolicy;
import org.smartdata.common.rule.RuleState;
import org.smartdata.metastore.tables.AccessCountTable;
import org.smartdata.metastore.tables.ActionDao;
import org.smartdata.metastore.tables.CacheFileDao;
import org.smartdata.metastore.tables.GroupsDao;
import org.smartdata.metastore.tables.RuleDao;
import org.smartdata.metastore.tables.StorageDao;
import org.smartdata.metastore.tables.CmdletDao;
import org.smartdata.metastore.tables.FileDao;
import org.smartdata.metastore.tables.UserDao;
import org.smartdata.metastore.tables.XattrDao;
import org.smartdata.metastore.utils.MetaStoreUtils;
import org.smartdata.metastore.tables.*;
import org.smartdata.metrics.FileAccessEvent;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
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
  private CacheFileDao cacheFileDao;
  private StorageDao storageDao;
  private UserDao userDao;
  private GroupsDao groupsDao;
  private XattrDao xattrDao;
  private AccessCountDao accessCountDao;

  public MetaStore(DBPool pool) throws MetaStoreException {
    this.pool = pool;
    ruleDao = new RuleDao(pool.getDataSource());
    cmdletDao = new CmdletDao(pool.getDataSource());
    actionDao = new ActionDao(pool.getDataSource());
    fileDao = new FileDao(pool.getDataSource());
    xattrDao = new XattrDao(pool.getDataSource());
    cacheFileDao = new CacheFileDao(pool.getDataSource());
    userDao = new UserDao(pool.getDataSource());
    storageDao = new StorageDao(pool.getDataSource());
    groupsDao = new GroupsDao(pool.getDataSource());
    accessCountDao = new AccessCountDao(pool.getDataSource());
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

  private class QueryHelper {
    // TODO need to remove
    private String query;
    private Connection conn;
    private boolean connProvided = false;
    private Statement statement;
    private ResultSet resultSet;
    private boolean closed = false;

    public QueryHelper(String query) throws MetaStoreException {
      this.query = query;
      conn = getConnection();
      if (conn == null) {
        throw new MetaStoreException("Invalid null connection");
      }
    }

    public QueryHelper(String query,
        Connection conn) throws MetaStoreException {
      this.query = query;
      this.conn = conn;
      connProvided = true;
      if (conn == null) {
        throw new MetaStoreException("Invalid null connection");
      }
    }

    public ResultSet executeQuery() throws MetaStoreException {
      try {
        statement = conn.createStatement();
        resultSet = statement.executeQuery(query);
      } catch (SQLException e) {
        throw new MetaStoreException(e);
      }
      return resultSet;
    }

    public int executeUpdate() throws MetaStoreException {
      try {
        statement = conn.createStatement();
        return statement.executeUpdate(query);
      } catch (SQLException e) {
        throw new MetaStoreException(e);
      }
    }

    public void execute() throws MetaStoreException {
      try {
        statement = conn.createStatement();
        statement.execute(query);
      } catch (SQLException e) {
        throw new MetaStoreException(e);
      }
    }

    public void close() throws MetaStoreException {
      if (closed) {
        return;
      }
      closed = true;

      try {
        if (resultSet != null && !resultSet.isClosed()) {
          resultSet.close();
        }

        if (statement != null && !statement.isClosed()) {
          statement.close();
        }
      } catch (SQLException e) {
        throw new MetaStoreException(e);
      }

      if (conn != null && !connProvided) {
        closeConnection(conn);
      }
    }
  }

  public Map<Long, Integer> getAccessCount(long startTime, long endTime,
      String countFilter) throws MetaStoreException {
    // TODO access file
    Map<Long, Integer> ret = new HashMap<>();
    String sqlGetTableNames = "SELECT table_name FROM access_count_tables "
        + "WHERE start_time >= " + startTime + " AND end_time <= " + endTime;
    Connection conn = getConnection();
    QueryHelper qhTableName = null;
    ResultSet rsTableNames = null;
    QueryHelper qhValues = null;
    ResultSet rsValues = null;
    try {
      qhTableName = new QueryHelper(sqlGetTableNames, conn);
      rsTableNames = qhTableName.executeQuery();
      List<String> tableNames = new LinkedList<>();
      try {
        while (rsTableNames.next()) {
          tableNames.add(rsTableNames.getString(1));
        }
      } catch (SQLException e) {
        throw new MetaStoreException(e);
      }
      qhTableName.close();

      if (tableNames.size() == 0) {
        return ret;
      }

      String sqlPrefix = "SELECT fid, SUM(count) AS count FROM (\n";
      String sqlUnion = "SELECT fid, count FROM \'"
          + tableNames.get(0) + "\'\n";
      for (int i = 1; i < tableNames.size(); i++) {
        sqlUnion += "UNION ALL\n" +
            "SELECT fid, count FROM \'" + tableNames.get(i) + "\'\n";
      }
      String sqlSufix = ") GROUP BY fid ";
      // TODO: safe check
      String sqlCountFilter =
          (countFilter == null || countFilter.length() == 0) ?
              "" :
              "HAVING SUM(count) " + countFilter;
      String sqlFinal = sqlPrefix + sqlUnion + sqlSufix + sqlCountFilter;

      qhValues = new QueryHelper(sqlFinal, conn);
      rsValues = qhValues.executeQuery();

      try {
        while (rsValues.next()) {
          ret.put(rsValues.getLong(1), rsValues.getInt(2));
        }
      } catch (SQLException e) {
        throw new MetaStoreException(e);
      }

      return ret;
    } finally {
      if (qhTableName != null) {
        qhTableName.close();
      }

      if (qhValues != null) {
        qhValues.close();
      }

      closeConnection(conn);
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
    fileDao.insert(files);
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
      StringBuilder unioned = new StringBuilder();
      while (tableIterator.hasNext()) {
        AccessCountTable table = tableIterator.next();
        if (tableIterator.hasNext()) {
          unioned
              .append("SELECT * FROM " + table.getTableName() + " UNION ALL ");
        } else {
          unioned.append("SELECT * FROM " + table.getTableName());
        }
      }
      String statement =
          String.format(
              "SELECT %s, SUM(%s) as %s FROM (%s) tmp GROUP BY %s ORDER BY %s DESC LIMIT %s",
              AccessCountDao.FILE_FIELD,
              AccessCountDao.ACCESSCOUNT_FIELD,
              AccessCountDao.ACCESSCOUNT_FIELD,
              unioned,
              AccessCountDao.FILE_FIELD,
              AccessCountDao.ACCESSCOUNT_FIELD,
              topNum);
      try {
        ResultSet resultSet = this.executeQuery(statement);
        Map<Long, Integer> accessCounts = new HashMap<>();
        while (resultSet.next()) {
          accessCounts.put(
              resultSet.getLong(AccessCountDao.FILE_FIELD),
              resultSet.getInt(AccessCountDao.ACCESSCOUNT_FIELD));
        }
        Map<Long, String> idToPath = this.getFilePaths(accessCounts.keySet());
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
            "CREATE VIEW %s AS SELECT %s, FLOOR(%s.%s * %s) AS %s FROM %s",
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
      execute("DROP TABLE " + tableName);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public int executeUpdate(String sql) throws MetaStoreException {
    QueryHelper queryHelper = new QueryHelper(sql);
    try {
      return queryHelper.executeUpdate();
    } finally {
      queryHelper.close();
    }
  }

  public void execute(String sql) throws MetaStoreException {
    QueryHelper queryHelper = new QueryHelper(sql);
    try {
      queryHelper.execute();
    } finally {
      queryHelper.close();
    }
  }

  //Todo: optimize
  public void execute(List<String> statements) throws MetaStoreException {
    for (String statement : statements) {
      this.execute(statement);
    }
  }

  public List<String> executeFilesPathQuery(
      String sql) throws MetaStoreException {
    List<String> paths = new LinkedList<>();
    QueryHelper queryHelper = new QueryHelper(sql);
    try {
      ResultSet res = queryHelper.executeQuery();
      try {
        while (res.next()) {
          paths.add(res.getString(1));
        }
      } catch (Exception e) {
        throw new MetaStoreException(e);
      }
      return paths;
    } finally {
      queryHelper.close();
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

  @VisibleForTesting
  public ResultSet executeQuery(String sqlQuery) throws MetaStoreException {
    Connection conn = getConnection();
    try {
      Statement s = conn.createStatement();
      return s.executeQuery(sqlQuery);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    } finally {
      closeConnection(conn);
    }
  }
}
