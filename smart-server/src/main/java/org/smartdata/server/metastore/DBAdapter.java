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
package org.smartdata.server.metastore;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.smartdata.common.CmdletState;
import org.smartdata.common.actions.ActionInfo;
import org.smartdata.common.cmdlet.CmdletInfo;
import org.smartdata.common.metastore.CachedFileStatus;
import org.smartdata.common.rule.RuleInfo;
import org.smartdata.common.rule.RuleState;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.server.metastore.tables.AccessCountTable;
import org.smartdata.server.utils.JsonUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
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

/**
 * Operations supported for upper functions.
 */
public class DBAdapter {

  private Connection connProvided = null;
  private DBPool pool = null;

  private Map<Integer, String> mapOwnerIdName = null;
  private Map<Integer, String> mapGroupIdName = null;
  private Map<Integer, String> mapStoragePolicyIdName = null;
  private Map<String, Integer> mapStoragePolicyNameId = null;
  private Map<String, StorageCapacity> mapStorageCapacity = null;

  @VisibleForTesting
  public DBAdapter(Connection conn) {
    connProvided = conn;
  }

  public DBAdapter(DBPool pool) throws IOException {
    this.pool = pool;
  }

  public Connection getConnection() throws SQLException {
    return pool != null ? pool.getConnection() : connProvided;
  }

  private void closeConnection(Connection conn) throws SQLException {
    if (pool != null) {
      pool.closeConnection(conn);
    }
  }

  private class QueryHelper {
    private String query;
    private Connection conn;
    private boolean connProvided = false;
    private Statement statement;
    private ResultSet resultSet;
    private boolean closed = false;

    public QueryHelper(String query) throws SQLException {
      this.query = query;
      conn = getConnection();
      if (conn == null) {
        throw new SQLException("Invalid null connection");
      }
    }

    public QueryHelper(String query, Connection conn) throws SQLException {
      this.query = query;
      this.conn = conn;
      connProvided = true;
      if (conn == null) {
        throw new SQLException("Invalid null connection");
      }
    }

    public ResultSet executeQuery() throws SQLException {
      statement = conn.createStatement();
      resultSet = statement.executeQuery(query);
      return resultSet;
    }

    public int executeUpdate() throws SQLException {
      statement = conn.createStatement();
      return statement.executeUpdate(query);
    }

    public void execute() throws SQLException {
      statement = conn.createStatement();
      statement.executeUpdate(query);
    }

    public void close() throws SQLException {
      if (closed) {
        return;
      }
      closed = true;

      if (resultSet != null && !resultSet.isClosed()) {
        resultSet.close();
      }

      if (statement != null && !statement.isClosed()) {
        statement.close();
      }

      if (conn != null && !connProvided) {
        closeConnection(conn);
      }
    }
  }

  public Map<Long, Integer> getAccessCount(long startTime, long endTime,
      String countFilter) throws SQLException {
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
      while (rsTableNames.next()) {
        tableNames.add(rsTableNames.getString(1));
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

      while (rsValues.next()) {
        ret.put(rsValues.getLong(1), rsValues.getInt(2));
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

  private synchronized void addUser(String userName) throws SQLException {
    String sql = String.format("INSERT INTO `owners` (owner_name) VALUES ('%s')", userName);
    this.execute(sql);
  }

  private synchronized void addGroup(String groupName) throws SQLException {
    String sql = String.format("INSERT INTO `groups` (group_name) VALUES ('%s')", groupName);
    this.execute(sql);
  }

  private void updateUsersMap() throws SQLException {
    String sql = "SELECT * FROM owners";
    QueryHelper queryHelper = new QueryHelper(sql);
    try {
      mapOwnerIdName = convertToMap(queryHelper.executeQuery(), "oid", "owner_name");
    } finally {
      queryHelper.close();
    }
  }

  private void updateGroupsMap() throws SQLException {
    String sql = "SELECT * FROM groups";
    QueryHelper queryHelper = new QueryHelper(sql);
    try {
      mapGroupIdName = convertToMap(queryHelper.executeQuery(), "gid", "group_name");
    } finally {
      queryHelper.close();
    }
  }

  /**
   * Store files info into database.
   *
   * @param files
   */
  public synchronized void insertFiles(FileStatusInternal[] files)
      throws SQLException {
    updateCache();
    Connection conn = getConnection();
    Statement s = null;
    try {
      s = conn.createStatement();
      for (FileStatusInternal file: files) {
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
        String statement =
            String.format(
                "INSERT INTO `files` (path, fid, length, block_replication, block_size,"
                    + " modification_time, access_time, is_dir, sid, oid, gid, permission)"
                    + " VALUES ('%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                file.getPath(),
                file.getFileId(),
                file.getLen(),
                file.getReplication(),
                file.getBlockSize(),
                file.getModificationTime(),
                file.getAccessTime(),
                booleanToInt(file.isDir()),
                file.getStoragePolicy(),
                getKey(mapOwnerIdName, file.getOwner()),
                getKey(mapGroupIdName, file.getGroup()),
                file.getPermission().toShort());
        s.addBatch(statement);
      }
      s.executeBatch();
    } finally {
      if (s != null) {
        s.close();
      }
      if (conn != null) {
        closeConnection(conn);
      }
    }
  }

  public int updateFileStoragePolicy(String path, String policyName)
      throws SQLException {
    if (mapStoragePolicyIdName == null) {
      updateCache();
    }
    if (!mapStoragePolicyNameId.containsKey(policyName)) {
      throw new SQLException("Unknown storage policy name '"
          + policyName + "'");
    }
    String sql = String.format(
        "UPDATE files SET sid = %d WHERE path = '%s';",
        mapStoragePolicyNameId.get(policyName), path);
    return executeUpdate(sql);
  }

  private int booleanToInt(boolean b) {
    return b ? 1 : 0;
  }

  private Integer getKey(Map<Integer, String> map, String value) {
    for (Integer key : map.keySet()) {
      if (map.get(key).equals(value)) {
        return key;
      }
    }
    return null;
  }

  public List<HdfsFileStatus> getFile() throws SQLException {
    String sql = "SELECT * FROM files";
    QueryHelper queryHelper = new QueryHelper(sql);
    try {
      ResultSet result = queryHelper.executeQuery();
      List<HdfsFileStatus> ret = convertFilesTableItem(result);
      return ret.size() > 0 ? ret : null;
    } finally {
      queryHelper.close();
    }
  }

  public HdfsFileStatus getFile(long fid) throws SQLException {
    String sql = "SELECT * FROM files WHERE fid = " + fid;
    QueryHelper queryHelper = new QueryHelper(sql);
    try {
      ResultSet result = queryHelper.executeQuery();
      List<HdfsFileStatus> ret = convertFilesTableItem(result);
      return ret.size() > 0 ? ret.get(0) : null;
    } finally {
      queryHelper.close();
    }
  }

  public Map<String, Long> getFileIDs(Collection<String> paths)
      throws SQLException {
    Map<String, Long> pathToId = new HashMap<>();
    List<String> values = new ArrayList<>();
    for (String path : paths) {
      values.add("'" + path + "'");
    }
    String in = StringUtils.join(values, ", ");
    String sql = "SELECT fid, path FROM files WHERE path IN (" + in + ")";
    QueryHelper queryHelper = new QueryHelper(sql);
    ResultSet result;
    try {
      result = queryHelper.executeQuery();
      while (result.next()) {
        pathToId.put(result.getString("path"),
            result.getLong("fid"));
      }
      return pathToId;
    } finally {
      queryHelper.close();
    }
  }

  public Map<Long, String> getFilePaths(Collection<Long> ids)
     throws SQLException {
    Map<Long, String> idToPath = new HashMap<>();
    List<String> values = new ArrayList<>();
    for (Long id : ids) {
      values.add("'" + id + "'");
    }
    String in = StringUtils.join(values, ", ");
    String sql = "SELECT fid, path FROM files WHERE fid IN (" + in + ")";
    QueryHelper queryHelper = new QueryHelper(sql);
    ResultSet result;
    try {
      result = queryHelper.executeQuery();
      while (result.next()) {
        idToPath.put(result.getLong("fid"),
          result.getString("path"));
      }
      return idToPath;
    } finally {
      queryHelper.close();
    }
  }

  public synchronized List<FileAccessInfo> getHotFiles(List<AccessCountTable> tables,
      int topNum) throws SQLException {
    Iterator<AccessCountTable> tableIterator = tables.iterator();
    if (tableIterator.hasNext()) {
      StringBuilder unioned = new StringBuilder();
      while (tableIterator.hasNext()) {
        AccessCountTable table = tableIterator.next();
        if (tableIterator.hasNext()) {
          unioned.append("SELECT * FROM " + table.getTableName() + " UNION ALL ");
        } else {
          unioned.append("SELECT * FROM " + table.getTableName());
        }
      }
      String statement =
        String.format(
          "SELECT %s, SUM(%s) as %s FROM (%s) tmp GROUP BY %s ORDER BY %s DESC LIMIT %s",
          AccessCountTable.FILE_FIELD,
          AccessCountTable.ACCESSCOUNT_FIELD,
          AccessCountTable.ACCESSCOUNT_FIELD,
          unioned,
          AccessCountTable.FILE_FIELD,
          AccessCountTable.ACCESSCOUNT_FIELD,
          topNum);
      ResultSet resultSet = this.executeQuery(statement);
      Map<Long, Integer> accessCounts = new HashMap<>();
      while (resultSet.next()) {
        accessCounts.put(
          resultSet.getLong(AccessCountTable.FILE_FIELD),
          resultSet.getInt(AccessCountTable.ACCESSCOUNT_FIELD));
      }
      Map<Long, String> idToPath = this.getFilePaths(accessCounts.keySet());
      List<FileAccessInfo> result = new ArrayList<>();
      for (Map.Entry<Long, Integer> entry : accessCounts.entrySet()) {
        Long fid = entry.getKey();
        if (idToPath.containsKey(fid)) {
          result.add(new FileAccessInfo(fid, idToPath.get(fid), accessCounts.get(fid)));
        }
      }
      return result;
    } else {
      return new ArrayList<>();
    }
  }

  public HdfsFileStatus getFile(String path) throws SQLException {
    String sql = "SELECT * FROM files WHERE path = \'" + path + "\'";
    QueryHelper queryHelper = new QueryHelper(sql);
    try {
      ResultSet result = queryHelper.executeQuery();
      List<HdfsFileStatus> ret = convertFilesTableItem(result);
      return ret.size() > 0 ? ret.get(0) : null;
    } finally {
      queryHelper.close();
    }
  }

  public void insertStoragesTable(StorageCapacity[] storages)
      throws SQLException {
    Connection conn = getConnection();
    Statement s = null;
    try {
      mapStorageCapacity = null;
      s = conn.createStatement();
      for (int i = 0; i < storages.length; i++) {
        String sql = "INSERT INTO `storages` (type, capacity, free) VALUES"
            + " ('" + storages[i].getType()
            + "','" + storages[i].getCapacity() + "','"
            + storages[i].getFree() + "')";
        s.addBatch(sql);
      }
      s.executeBatch();
    } finally {
      if (s != null) {
        s.close();
      }
      closeConnection(conn);
    }
  }

  public StorageCapacity getStorageCapacity(String type) throws SQLException {
    updateCache();
    return mapStorageCapacity.get(type);
  }

  public synchronized boolean updateStoragesTable(String type,
      Long capacity, Long free) throws SQLException {
    String sql = null;
    String sqlPrefix = "UPDATE storages SET";
    String sqlCapacity = (capacity != null) ? ", capacity = '"
        + capacity + "'" : null;
    String sqlFree = (free != null) ? ", free = '" + free + "' " : null;
    String sqlSuffix = "WHERE type = '" + type + "';";
    if (capacity != null || free != null) {
      sql = sqlPrefix + sqlCapacity + sqlFree + sqlSuffix;
      sql = sql.replaceFirst(",", "");
    }
    QueryHelper queryHelper = new QueryHelper(sql);
    try {
      mapStorageCapacity = null;
      return queryHelper.executeUpdate() == 1;
    } finally {
      queryHelper.close();
    }
  }

  /**
   * Convert query result into HdfsFileStatus list.
   * Note: Some of the info in HdfsFileStatus are not the same
   * as stored in NN.
   *
   * @param resultSet
   * @return
   */
  public List<HdfsFileStatus> convertFilesTableItem(ResultSet resultSet)
      throws SQLException {
    List<HdfsFileStatus> ret = new LinkedList<>();
    if (resultSet == null) {
      return ret;
    }
    updateCache();

    while (resultSet.next()) {
      HdfsFileStatus status = new HdfsFileStatus(
          resultSet.getLong("length"),
          resultSet.getBoolean("is_dir"),
          resultSet.getShort("block_replication"),
          resultSet.getLong("block_size"),
          resultSet.getLong("modification_time"),
          resultSet.getLong("access_time"),
          new FsPermission(resultSet.getShort("permission")),
          mapOwnerIdName.get(resultSet.getShort("oid")),
          mapGroupIdName.get(resultSet.getShort("gid")),
          null, // Not tracked for now
          resultSet.getString("path").getBytes(),
          resultSet.getLong("fid"),
          0,    // Not tracked for now, set to 0
          null, // Not tracked for now, set to null
          resultSet.getByte("sid"));
      ret.add(status);
    }
    return ret;
  }

  private void updateCache() throws SQLException {
    if (mapOwnerIdName == null) {
      String sql = "SELECT * FROM owners";
      QueryHelper queryHelper = new QueryHelper(sql);
      try {
        mapOwnerIdName = convertToMap(queryHelper.executeQuery(), "oid", "owner_name");
      } finally {
        queryHelper.close();
      }
    }

    if (mapGroupIdName == null) {
      String sql = "SELECT * FROM groups";
      QueryHelper queryHelper = new QueryHelper(sql);
      try {
        mapGroupIdName = convertToMap(queryHelper.executeQuery(), "gid", "group_name");
      } finally {
        queryHelper.close();
      }
    }

    if (mapStoragePolicyIdName == null) {
      mapStoragePolicyNameId = null;
      String sql = "SELECT * FROM storage_policy";
      QueryHelper queryHelper = new QueryHelper(sql);
      try {
        mapStoragePolicyIdName = convertToMap(queryHelper.executeQuery(), "sid", "policy_name");
        mapStoragePolicyNameId = new HashMap<>();
        for (Integer key : mapStoragePolicyIdName.keySet()) {
          mapStoragePolicyNameId.put(mapStoragePolicyIdName.get(key), key);
        }
      } finally {
        queryHelper.close();
      }
    }

    if (mapStorageCapacity == null) {
      String sql = "SELECT * FROM storages";
      QueryHelper queryHelper = new QueryHelper(sql);
      try {
        mapStorageCapacity =
            convertStorageTablesItem(queryHelper.executeQuery());
      } finally {
        queryHelper.close();
      }
    }
  }

  private Map<String, StorageCapacity> convertStorageTablesItem(
      ResultSet resultSet) throws SQLException {
    Map<String, StorageCapacity> map = new HashMap<>();
    if (resultSet == null) {
      return map;
    }

    while (resultSet.next()) {
      String type = resultSet.getString(1);
      StorageCapacity storage = new StorageCapacity(
          resultSet.getString(1),
          resultSet.getLong(2),
          resultSet.getLong(3));
      map.put(type, storage);
    }
    return map;
  }

  private Map<Integer, String> convertToMap(ResultSet resultSet, String keyIndex, String valueIndex)
      throws SQLException {
    Map<Integer, String> ret = new HashMap<>();
    if (resultSet == null) {
      return ret;
    }

    while (resultSet.next()) {
      // TODO: Tests for this
      ret.put(resultSet.getInt(keyIndex), resultSet.getString(valueIndex));
    }
    return ret;
  }

  public synchronized void insertCachedFiles(long fid, String path, long fromTime,
      long lastAccessTime, int numAccessed) throws SQLException {
    String sql =
        String.format(
            "INSERT INTO `cached_files` (fid, path, from_time, last_access_time,"
                + " num_accessed) VALUES (%s, '%s', %s, %s, %s);",
            fid, path, fromTime, lastAccessTime, numAccessed);
    execute(sql);
  }

  public synchronized void insertCachedFiles(List<CachedFileStatus> s)
      throws SQLException {
    Connection conn = getConnection();
    Statement st = null;
    try {
      st = conn.createStatement();
      for (CachedFileStatus c : s) {
        String sql =
            String.format(
                "INSERT INTO `cached_files` (fid, path, from_time, last_access_time, "
                    + "num_accessed) VALUES (%s, '%s', %s, %s, %s);",
                c.getFid(),
                c.getPath(),
                c.getFromTime(),
                c.getLastAccessTime(),
                c.getNumAccessed());
        st.addBatch(sql);
      }
      st.executeBatch();
    } finally {
      if (st != null) {
        st.close();
      }
      closeConnection(conn);
    }
  }

  public void deleteAllCachedFile() throws SQLException {
    String sql = "DELETE from `cached_files`";
    execute(sql);
  }

  public void updateCachedFiles(Map<String, Long> pathToIds, List<FileAccessEvent> events)
      throws SQLException {
    Map<Long, CachedFileStatus> idToStatus = new HashMap<>();
    List<CachedFileStatus> cachedFileStatuses = this.getCachedFileStatus();
    for (CachedFileStatus status : cachedFileStatuses) {
      idToStatus.put(status.getFid(), status);
    }
    Collection<Long> cachedIds = idToStatus.keySet();
    Collection<Long> needToUpdate = CollectionUtils.intersection(cachedIds, pathToIds.values());
    if (!needToUpdate.isEmpty()) {
      Map<Long, Integer> idToCount = new HashMap<>();
      Map<Long, Long> idToLastTime = new HashMap<>();
      for (FileAccessEvent event : events) {
        Long fid = pathToIds.get(event.getPath());
        if (needToUpdate.contains(fid)) {
          if (!idToCount.containsKey(fid)) {
            idToCount.put(fid, 0);
          }
          idToCount.put(fid, idToCount.get(fid) + 1);
          if (!idToLastTime.containsKey(fid)) {
            idToLastTime.put(fid, event.getTimestamp());
          }
          idToLastTime.put(fid, Math.max(event.getTimestamp(), idToLastTime.get(fid)));
        }
      }
      for (Long fid : needToUpdate) {
        Integer newAccessCount = idToStatus.get(fid).getNumAccessed() + idToCount.get(fid);
        this.updateCachedFiles(fid, null, idToLastTime.get(fid), newAccessCount);
      }
    }
  }

  public void deleteCachedFile(long fid) throws SQLException {
    String sql = String.format("DELETE from `cached_files` WHERE fid = '%s'", fid);
    execute(sql);
  }

  public synchronized boolean updateCachedFiles(Long fid, Long fromTime,
      Long lastAccessTime, Integer numAccessed) throws SQLException {
    StringBuffer sb = new StringBuffer("UPDATE cached_files SET");
    if (fromTime != null) {
      sb.append(" from_time = ").append(fid).append(",");
    }
    if (lastAccessTime != null) {
      sb.append(" last_access_time = ").append(lastAccessTime).append(",");
    }
    if (numAccessed != null) {
      sb.append(" num_accessed = ").append(numAccessed).append(",");
    }
    int idx = sb.lastIndexOf(",");
    sb.replace(idx, idx + 1, "");
    sb.append(" WHERE fid = ").append(fid).append(";");

    QueryHelper queryHelper = new QueryHelper(sb.toString());
    try {
      return queryHelper.executeUpdate() == 1;
    } finally {
      queryHelper.close();
    }
  }

  public List<CachedFileStatus> getCachedFileStatus() throws SQLException {
    String sql = "SELECT * FROM `cached_files`";
    return getCachedFileStatus(sql);
  }

  public List<Long> getCachedFids() throws SQLException {
    String sql = "SELECT fid FROM `cached_files`";
    QueryHelper queryHelper = new QueryHelper(sql);
    List<Long> ret = new LinkedList<>();
    try {
      ResultSet resultSet = queryHelper.executeQuery();
      while (resultSet.next()) {
        ret.add(resultSet.getLong("fid"));
      }
      return ret;
    } finally {
      queryHelper.close();
    }
  }

  public CachedFileStatus getCachedFileStatus(long fid) throws SQLException {
    String sql = "SELECT * FROM cached_files WHERE fid = " + fid;
    List<CachedFileStatus> s = getCachedFileStatus(sql);
    return s != null ? s.get(0) : null;
  }

  public void createProportionView(AccessCountTable dest, AccessCountTable source)
      throws SQLException {
    double percentage =
        ((double) dest.getEndTime() - dest.getStartTime())
            / (source.getEndTime() - source.getStartTime());
    String sql =
        String.format(
            "CREATE VIEW %s AS SELECT %s, FLOOR(%s.%s * %s) AS %s FROM %s",
            dest.getTableName(),
            AccessCountTable.FILE_FIELD,
            source.getTableName(),
            AccessCountTable.ACCESSCOUNT_FIELD,
            percentage,
            AccessCountTable.ACCESSCOUNT_FIELD,
            source.getTableName());
    execute(sql);
  }

  public void dropTable(String tableName) throws SQLException {
    execute("DROP TABLE " + tableName);
  }

  private List<CachedFileStatus> getCachedFileStatus(String sql)
      throws SQLException {
    QueryHelper queryHelper = new QueryHelper(sql);
    List<CachedFileStatus> ret = new LinkedList<>();

    try {
      ResultSet resultSet = queryHelper.executeQuery();
      while (resultSet.next()) {
        CachedFileStatus f = new CachedFileStatus(
            resultSet.getLong("fid"),
            resultSet.getString("path"),
            resultSet.getLong("from_time"),
            resultSet.getLong("last_access_time"),
            resultSet.getInt("num_accessed")
        );
        ret.add(f);
      }
      return ret;
    } finally {
      queryHelper.close();
    }
  }

  public int executeUpdate(String sql) throws SQLException {
    QueryHelper queryHelper = new QueryHelper(sql);
    try {
      return queryHelper.executeUpdate();
    } finally {
      queryHelper.close();
    }
  }

  public void execute(String sql) throws SQLException {
    QueryHelper queryHelper = new QueryHelper(sql);
    try {
      queryHelper.execute();
    } finally {
      queryHelper.close();
    }
  }

  //Todo: optimize
  public void execute(List<String> statements) throws SQLException {
    for (String statement : statements) {
      this.execute(statement);
    }
  }

  public List<String> executeFilesPathQuery(String sql) throws SQLException {
    List<String> paths = new LinkedList<>();
    QueryHelper queryHelper = new QueryHelper(sql);
    try {
      ResultSet res = queryHelper.executeQuery();
      while (res.next()) {
        paths.add(res.getString(1));
      }
      return paths;
    } finally {
      queryHelper.close();
    }
  }

  public synchronized boolean insertNewRule(RuleInfo info)
      throws SQLException {
    long ruleId = 0;
    if (info.getSubmitTime() == 0) {
      info.setSubmitTime(System.currentTimeMillis());
    }

    String sql = "INSERT INTO rules (state, rule_text, submit_time, "
        + "checked_count, cmdlets_generated"
        + (info.getLastCheckTime() == 0 ? "" : ", last_check_time")
        + ") VALUES ("
        + info.getState().getValue()
        + ", '" + info.getRuleText() + "'" // TODO: take care of '
        + ", " + info.getSubmitTime()
        + ", " + info.getNumChecked()
        + ", " + info.getNumCmdsGen()
        + (info.getLastCheckTime() == 0 ? "" : ", " + info.getLastCheckTime())
        + ");";

    QueryHelper queryHelperInsert = new QueryHelper(sql);
    try {
      queryHelperInsert.execute();
    } finally {
      queryHelperInsert.close();
    }

    QueryHelper queryHelper = new QueryHelper("SELECT MAX(id) FROM rules;");
    try {
      ResultSet rs = queryHelper.executeQuery();
      if (rs.next()) {
        ruleId = rs.getLong(1);
        info.setId(ruleId);
        return true;
      } else {
        return false;
      }
    } finally {
      queryHelper.close();
    }
  }

  public synchronized boolean updateRuleInfo(long ruleId, RuleState rs,
      long lastCheckTime, long checkedCount, int cmdletsGen)
      throws SQLException {
    StringBuffer sb = new StringBuffer("UPDATE rules SET");
    if (rs != null) {
      sb.append(" state = ").append(rs.getValue()).append(",");
    }
    if (lastCheckTime != 0) {
      sb.append(" last_check_time = ").append(lastCheckTime).append(",");
    }
    if (checkedCount != 0) {
      sb.append(" checked_count = checked_count + ")
          .append(checkedCount).append(",");
    }
    if (cmdletsGen != 0) {
      sb.append(" cmdlets_generated = cmdlets_generated + ")
          .append(cmdletsGen).append(",");
    }
    int idx = sb.lastIndexOf(",");
    sb.replace(idx, idx + 1, "");
    sb.append(" WHERE id = ").append(ruleId).append(";");

    return 1 == executeUpdate(sb.toString());
  }

  public RuleInfo getRuleInfo(long ruleId) throws SQLException {
    String sql = "SELECT * FROM rules WHERE id = " + ruleId;
    List<RuleInfo> infos = doGetRuleInfo(sql);
    return infos.size() == 1 ? infos.get(0) : null;
  }

  public List<RuleInfo> getRuleInfo() throws SQLException {
    String sql = "SELECT * FROM rules";
    return doGetRuleInfo(sql);
  }

  private List<RuleInfo> doGetRuleInfo(String sql) throws SQLException {
    QueryHelper queryHelper = new QueryHelper(sql);
    List<RuleInfo> infos = new LinkedList<>();
    try {
      ResultSet rs = queryHelper.executeQuery();
      while (rs.next()) {
        infos.add(new RuleInfo(
            rs.getLong("id"),
            rs.getLong("submit_time"),
            rs.getString("rule_text"),
            RuleState.fromValue((int) rs.getByte("state")),
            rs.getLong("checked_count"),
            rs.getLong("cmdlets_generated"),
            rs.getLong("last_check_time")
        ));
      }
      return infos;
    } finally {
      queryHelper.close();
    }
  }

  public synchronized void insertCmdletsTable(CmdletInfo[] cmdlets)
      throws SQLException {
    Connection conn = getConnection();
    Statement s = null;
    try {
      s = conn.createStatement();
      for (int i = 0; i < cmdlets.length; i++) {
        String sql = "INSERT INTO cmdlets (rid, aids, state, "
            + "parameters, generate_time, state_changed_time) "
            + "VALUES('" + cmdlets[i].getRid() + "', '"
            + StringUtils.join(cmdlets[i].getAidsString(), ",") + "', '"
            + cmdlets[i].getState().getValue() + "', '"
            + cmdlets[i].getParameters() + "', '"
            + cmdlets[i].getGenerateTime() + "', '"
            + cmdlets[i].getStateChangedTime() + "');";
        s.addBatch(sql);
      }
      s.executeBatch();
    } finally {
      if (s != null && !s.isClosed()) {
        s.close();
      }
      closeConnection(conn);
    }
  }

  public synchronized void insertCmdletTable(CmdletInfo cmdlet)
      throws SQLException {
    // Insert single cmdlet into cmdlets, update cmdlet.id with latest id
    String sql = "INSERT INTO cmdlets (rid, aids, state, "
        + "parameters, generate_time, state_changed_time) "
        + "VALUES('" + cmdlet.getRid() + "', '"
        + StringUtils.join(cmdlet.getAidsString(), ",") + "', '"
        + cmdlet.getState().getValue() + "', '"
        + cmdlet.getParameters() + "', '"
        + cmdlet.getGenerateTime() + "', '"
        + cmdlet.getStateChangedTime() + "');";
    execute(sql);
  }

  public long getMaxCmdletId() throws SQLException {
    QueryHelper queryHelper = new QueryHelper("SELECT MAX(cid) FROM cmdlets;");
    long maxId = 0;
    try {
      ResultSet rs = queryHelper.executeQuery();
      if (rs.next()) {
        maxId = rs.getLong(1) + 1;
      }
    } catch (NullPointerException e) {
      maxId = 0;
    } finally {
      queryHelper.close();
    }
    return maxId;
  }

  public List<CmdletInfo> getCmdletsTableItem(String cidCondition,
      String ridCondition, CmdletState state) throws SQLException {
    String sqlPrefix = "SELECT * FROM cmdlets WHERE ";
    String sqlCid = (cidCondition == null) ? "" : "AND cid " + cidCondition;
    String sqlRid = (ridCondition == null) ? "" : "AND rid " + ridCondition;
    String sqlState = (state == null) ? "" : "AND state = " + state.getValue();
    String sqlFinal = "";
    if (cidCondition != null || ridCondition != null || state != null) {
      sqlFinal = sqlPrefix + sqlCid + sqlRid + sqlState;
      sqlFinal = sqlFinal.replaceFirst("AND ", "");
    } else {
      sqlFinal = sqlPrefix.replaceFirst("WHERE ", "");
    }
    return getCmdlets(sqlFinal);
  }

  private List<CmdletInfo> getCmdlets(String sql) throws SQLException {
    QueryHelper queryHelper = new QueryHelper(sql);
    try {
      ResultSet result = queryHelper.executeQuery();
      List<CmdletInfo> ret = convertCmdletsTableItem(result);
      return ret;
    } finally {
      queryHelper.close();
    }
  }

  public boolean updateCmdletStatus(long cid, long rid, CmdletState state)
      throws SQLException {
    StringBuffer sb = new StringBuffer("UPDATE cmdlets SET");
    if (state != null) {
      sb.append(" state = ").append(state.getValue()).append(",");
      sb.append(" state_changed_time = ").append(System.currentTimeMillis()).append(",");
    }
    int idx = sb.lastIndexOf(",");
    sb.replace(idx, idx + 1, "");
    sb.append(" WHERE cid = ").append(cid).append(" AND ").append("rid = ")
        .append(rid).append(";");
    QueryHelper queryHelper = new QueryHelper(sb.toString());
    try {
      return queryHelper.executeUpdate() == 1;
    } finally {
      queryHelper.close();
    }
  }

  public void deleteCmdlet(long cid) throws SQLException {
    String sql = String.format("DELETE from cmdlets WHERE cid = %d;", cid);
    execute(sql);
  }

  private List<CmdletInfo> convertCmdletsTableItem(ResultSet resultSet)
      throws SQLException {
    List<CmdletInfo> ret = new LinkedList<>();
    if (resultSet == null) {
      return ret;
    }

    while (resultSet.next()) {
      CmdletInfo cmdlets = new CmdletInfo(
          resultSet.getLong("cid"),
          resultSet.getLong("rid"),
          convertStringListToLong(resultSet.getString("aids").split(",")),
          CmdletState.fromValue((int) resultSet.getByte("state")),
          resultSet.getString("parameters"),
          resultSet.getLong("generate_time"),
          resultSet.getLong("state_changed_time")
      );
      ret.add(cmdlets);
    }
    return ret;
  }

  private List<Long> convertStringListToLong(String[] strings) {
    List<Long> ret = new ArrayList<>();
    try {
      for (String s : strings) {
        ret.add(Long.valueOf(s));
      }
    } catch (NumberFormatException e) {
      // Return empty
      ret.clear();
    }
    return ret;
  }

  private String generateActionSQL(ActionInfo actionInfo) {
    String sql = "INSERT INTO actions (aid, cid, action_name, args, "
        + "result, log, successful, create_time, finished, finish_time, progress) "
        + "VALUES('" + actionInfo.getActionId() + "', '"
        + actionInfo.getCmdletId() + "', '"
        + actionInfo.getActionName() + "', '"
        + JsonUtil.toJsonString(actionInfo.getArgs()) + "', '"
        + actionInfo.getResult() + "', '"
        + actionInfo.getLog() + "', '"
        + booleanToInt(actionInfo.isSuccessful()) + "', '"
        + actionInfo.getCreateTime() + "', '"
        + booleanToInt(actionInfo.isFinished()) + "', '"
        + actionInfo.getFinishTime() + "', '"
        + String.valueOf(actionInfo.getProgress()) + "');";
    return sql;
  }

  public synchronized void insertActionsTable(ActionInfo[] actionInfos)
      throws SQLException {
    Connection conn = getConnection();
    Statement s = null;
    try {
      s = conn.createStatement();
      for (int i = 0; i < actionInfos.length; i++) {
        s.addBatch(generateActionSQL(actionInfos[i]));
      }
      s.executeBatch();
    } finally {
      if (s != null && !s.isClosed()) {
        s.close();
      }
      closeConnection(conn);
    }
  }

  public synchronized void insertActionTable(ActionInfo actionInfo)
      throws SQLException {
    String sql = generateActionSQL(actionInfo);
    execute(sql);
  }

  public synchronized void updateActionsTable(ActionInfo[] actionInfos)
      throws SQLException {
    Connection conn = getConnection();
    Statement s = null;
    // Update result, log, successful, create_time, finished, finish_time, progress
    try {
      s = conn.createStatement();
      for (int i = 0; i < actionInfos.length; i++) {
        StringBuffer sb = new StringBuffer("UPDATE actions SET");
        if (actionInfos[i].getResult().length() != 0) {
          sb.append(" result = '").append(actionInfos[i].getResult()).append("',");
        }
        if (actionInfos[i].getLog().length() != 0) {
          sb.append(" log = '").append(actionInfos[i].getLog()).append("',");
        }
        sb.append(" successful = ").append(booleanToInt(actionInfos[i].isSuccessful())).append(",");
        sb.append(" create_time = ").append(actionInfos[i].getCreateTime()).append(",");
        sb.append(" finished = ").append(booleanToInt(actionInfos[i].isFinished())).append(",");
        sb.append(" finish_time = ").append(actionInfos[i].getFinishTime()).append(",");
        sb.append(" progress = ").append(String.valueOf(actionInfos[i].getProgress()));
        sb.append(" WHERE aid = ").append(actionInfos[i].getActionId()).append(";");
        s.addBatch(sb.toString());
        System.out.println(sb.toString());
      }
      s.executeBatch();
    } finally {
      if (s != null && !s.isClosed()) {
        s.close();
      }
      closeConnection(conn);
    }
  }

  public List<ActionInfo> getNewCreatedActionsTableItem(int size) throws SQLException {
    if (size <= 0) {
      return new ArrayList<>();
    }
    // In DESC order
    // Only list finished actions
    String sqlFinal = "SELECT * FROM actions WHERE finished = 1"
        + String.format(" ORDER by create_time DESC limit %d", size);
    return getActions(sqlFinal);
  }

  public List<ActionInfo> getActionsTableItem(List<Long> aids) throws SQLException {
    if (aids == null || aids.size() == 0){
      return null;
    }
    String sql = "SELECT * FROM actions WHERE aid IN (";
    List<String> ret = new ArrayList<>();
    for (Long aid:aids) {
      ret.add(String.valueOf(aid));
    }
    sql = sql + StringUtils.join(ret, ",") + ")";
    return getActions(sql);
  }

  public List<ActionInfo> getActionsTableItem(String aidCondition,
      String cidCondition) throws SQLException {
    String sqlPrefix = "SELECT * FROM actions WHERE ";
    String sqlAid = (aidCondition == null) ? "" : "AND aid " + aidCondition;
    String sqlCid = (cidCondition == null) ? "" : "AND cid " + cidCondition;
    String sqlFinal = "";
    if (aidCondition != null || cidCondition != null) {
      sqlFinal = sqlPrefix + sqlAid + sqlCid;
      sqlFinal = sqlFinal.replaceFirst("AND ", "");
    } else {
      sqlFinal = sqlPrefix.replaceFirst("WHERE ", "");
    }
    return getActions(sqlFinal);
  }

  public long getMaxActionId() throws SQLException {
    QueryHelper queryHelper = new QueryHelper("SELECT MAX(aid) FROM actions;");
    long maxId = 0;
    try {
      ResultSet rs = queryHelper.executeQuery();
      if (rs.next()) {
        maxId = rs.getLong(1) + 1;
      }
    } catch (NullPointerException e) {
      maxId = 0;
    } finally {
      queryHelper.close();
    }
    return maxId;
  }

  private List<ActionInfo> getActions(String sql) throws SQLException {
    QueryHelper queryHelper = new QueryHelper(sql);
    List<ActionInfo> ret = new ArrayList<>();
    try {
      ResultSet result = queryHelper.executeQuery();
      ret = convertActionsTableItem(result);
    } finally {
      queryHelper.close();
    }
    return ret;
  }

  private List<ActionInfo> convertActionsTableItem(ResultSet resultSet)
      throws SQLException {
    List<ActionInfo> ret = new LinkedList<>();
    if (resultSet == null) {
      return ret;
    }

    while (resultSet.next()) {
      ActionInfo actionInfo;
      actionInfo = new ActionInfo(
          resultSet.getLong("aid"),
          resultSet.getLong("cid"),
          resultSet.getString("action_name"),
          JsonUtil.toStringStringMap(
              resultSet.getString("args")),
          resultSet.getString("result"),
          resultSet.getString("log"),
          resultSet.getBoolean("successful"),
          resultSet.getLong("create_time"),
          resultSet.getBoolean("finished"),
          resultSet.getLong("finish_time"),
          resultSet.getFloat("progress")
      );
      ret.add(actionInfo);
    }
    return ret;
  }

  public synchronized void insertStoragePolicyTable(StoragePolicy s)
      throws SQLException {
    String sql = "INSERT INTO `storage_policy` (sid, policy_name) VALUES('"
        + s.getSid() + "','" + s.getPolicyName() + "');";
    mapStoragePolicyIdName = null;
    execute(sql);
  }

  public String getStoragePolicyName(int sid) throws SQLException {
    updateCache();
    return mapStoragePolicyIdName.get(sid);
  }

  public Integer getStoragePolicyID(String policyName) throws SQLException {
    updateCache();
    return getKey(mapStoragePolicyIdName, policyName);
  }

  public synchronized boolean insertXattrTable(Long fid, Map<String,
      byte[]> map) throws SQLException {
    String sql = "INSERT INTO xattr (fid, namespace, name, value) "
        + "VALUES (?, ?, ?, ?)";
    Connection conn = getConnection();
    PreparedStatement p = null;
    try {
      conn.setAutoCommit(false);
      p = conn.prepareStatement(sql);
      for (Map.Entry<String, byte[]> e : map.entrySet()) {
        XAttr xa = XAttrHelper.buildXAttr(e.getKey(), e.getValue());
        p.setLong(1, fid);
        p.setString(2, String.valueOf(xa.getNameSpace()));
        p.setString(3, xa.getName());
        p.setBytes(4, xa.getValue());
        p.addBatch();
      }
      int[] i = p.executeBatch();
      p.close();
      p = null;
      conn.commit();
      conn.setAutoCommit(true);
      if (i.length == map.size()) {
        return true;
      } else {
        return false;
      }
    } finally {
      if (p != null && !p.isClosed()) {
        p.close();
      }
      closeConnection(conn);
    }
  }

  public Map<String, byte[]> getXattrTable(Long fid) throws SQLException {
    String sql =
        String.format("SELECT * FROM xattr WHERE fid = %s;", fid);
    return getXattrTable(sql);
  }

  private Map<String, byte[]> getXattrTable(String sql) throws SQLException {
    QueryHelper queryHelper = new QueryHelper(sql);
    ResultSet rs;
    List<XAttr> list = new LinkedList<>();
    try {
      rs = queryHelper.executeQuery();
      while (rs.next()) {
        XAttr xAttr = new XAttr.Builder().setNameSpace(
            XAttr.NameSpace.valueOf(rs.getString("namespace")))
            .setName(rs.getString("name"))
            .setValue(rs.getBytes("value")).build();
        list.add(xAttr);
      }
      return XAttrHelper.buildXAttrMap(list);
    } finally {
      queryHelper.close();
    }
  }

  public void dropAllTables() throws SQLException {
    Connection conn = getConnection();
    try {
      String url = conn.getMetaData().getURL();
      if (url.startsWith(MetaUtil.SQLITE_URL_PREFIX)) {
        dropAllTablesSqlite(conn);
      } else if (url.startsWith(MetaUtil.MYSQL_URL_PREFIX)) {
        dropAllTablesMysql(conn, url);
      } else {
        throw new SQLException("Unsupported database");
      }
    } finally {
      closeConnection(conn);
    }
  }

  public void dropAllTablesSqlite(Connection conn) throws SQLException {
    try {
      Statement s = conn.createStatement();
      ResultSet rs = s.executeQuery("SELECT tbl_name FROM sqlite_master;");
      List<String> list = new ArrayList<>();
      while (rs.next()) {
        list.add(rs.getString(1));
      }
      for (String tb : list) {
        if (!"sqlite_sequence".equals(tb)) {
          s.execute("DROP TABLE IF EXISTS '" + tb + "';");
        }
      }
    } finally {
      closeConnection(conn);
    }
  }

  public void dropAllTablesMysql(Connection conn, String url) throws SQLException {
    Statement stat = conn.createStatement();
    if(!url.contains("?")) {
      throw new SQLException("Invalid MySQL url without db_name");
    }
    String dbName = url.substring(url.indexOf("/", 13) + 1, url.indexOf("?"));
    ResultSet rs = stat.executeQuery("SELECT TABLE_NAME FROM "
        + "INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + dbName + "';");
    List<String> list = new ArrayList<>();
    while (rs.next()) {
      list.add(rs.getString(1));
    }
    for (String tb : list) {
      stat.execute("DROP TABLE IF EXISTS '" + tb + "';");
    }
  }

  public synchronized void initializeDataBase() throws SQLException {
    Connection conn = getConnection();
    try {
      MetaUtil.initializeDataBase(conn);
    } finally {
      closeConnection(conn);
    }
  }

  public synchronized void formatDataBase() throws SQLException {
    dropAllTables();
    initializeDataBase();
  }

  @VisibleForTesting
  public ResultSet executeQuery(String sqlQuery) throws SQLException {
    Connection conn = getConnection();
    try {
      Statement s = conn.createStatement();
      return s.executeQuery(sqlQuery);
    } finally {
      closeConnection(conn);
    }
  }
}
