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
package org.smartdata.server.metastore.sql;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.smartdata.common.CommandState;
import org.smartdata.common.command.CommandInfo;
import org.smartdata.common.command.actions.ActionType;
import org.smartdata.common.metastore.sql.CachedFileStatus;
import org.smartdata.common.rule.RuleInfo;
import org.smartdata.common.rule.RuleState;
import org.smartdata.server.metastore.sql.tables.AccessCountTable;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
      if (conn== null) {
        throw new SQLException("Invalid null connection");
      }
    }

    public QueryHelper(String query, Connection conn) throws SQLException {
      this.query = query;
      this.conn = conn;
      connProvided = true;
      if (conn== null) {
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
      for (int i = 0; i < files.length; i++) {
        String sql = "INSERT INTO `files` (path, fid, length, "
            + "block_replication,"
            + " block_size, modification_time, access_time, is_dir, sid, oid, "
            + "gid, permission) "
            + "VALUES ('" + files[i].getPath()
            + "','" + files[i].getFileId() + "','" + files[i].getLen() + "','"
            + files[i].getReplication() + "','" + files[i].getBlockSize()
            + "','" + files[i].getModificationTime() + "','"
            + files[i].getAccessTime()
            + "','" + booleanToInt(files[i].isDir()) + "','"
            + files[i].getStoragePolicy() + "','"
            + getKey(mapOwnerIdName, files[i].getOwner()) + "','"
            + getKey(mapGroupIdName, files[i].getGroup()) + "','"
            + files[i].getPermission().toShort() + "');";
        s.addBatch(sql);
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

  private int booleanToInt(boolean b) {
    return b ? 1 : 0;
  }

  private Integer getKey(Map<Integer, String> map, String value) {
    for (Integer key: map.keySet()) {
      if (map.get(key).equals(value)) {
        return key;
      }
    }
    return null;
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
    for(String path: paths) {
      values.add("'" + path + "'");
    }
    String in = String.join(", ", values);
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
   *       as stored in NN.
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
        mapOwnerIdName = convertToMap(queryHelper.executeQuery());
      } finally {
        queryHelper.close();
      }
    }

    if (mapGroupIdName == null) {
      String sql = "SELECT * FROM groups";
      QueryHelper queryHelper = new QueryHelper(sql);
      try {
        mapGroupIdName = convertToMap(queryHelper.executeQuery());
      } finally {
        queryHelper.close();
      }
    }

    if (mapStoragePolicyIdName == null) {
      String sql = "SELECT * FROM storage_policy";
      QueryHelper queryHelper = new QueryHelper(sql);
      try {
        mapStoragePolicyIdName = convertToMap(queryHelper.executeQuery());
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

  private Map<Integer, String> convertToMap(ResultSet resultSet)
      throws SQLException {
    Map<Integer, String> ret = new HashMap<>();
    if (resultSet == null) {
      return ret;
    }

    while (resultSet.next()) {
      // TODO: Tests for this
      ret.put(resultSet.getInt(1), resultSet.getString(2));
    }
    return ret;
  }

  public synchronized void insertCachedFiles(long fid, long fromTime,
      long lastAccessTime, int numAccessed) throws SQLException {
    String sql = "INSERT INTO cached_files (fid, from_time, "
        + "last_access_time, num_accessed) VALUES (" + fid + ","
        + fromTime + "," + lastAccessTime + ","
        + numAccessed + ")";
    execute(sql);
  }

  public synchronized void insertCachedFiles(List<CachedFileStatus> s)
      throws SQLException {
    Connection conn = getConnection();
    Statement st = null;
    try {
      st = conn.createStatement();
      for (CachedFileStatus c : s) {
        String sql = "INSERT INTO cached_files (fid, from_time, "
            + "last_access_time, num_accessed) VALUES (" + c.getFid() + ","
            + c.getFromTime() + "," + c.getLastAccessTime() + ","
            + c.getNumAccessed() + ")";
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
    String sql = "SELECT * FROM cached_files";
    return getCachedFileStatus(sql);
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

  private List<CachedFileStatus> getCachedFileStatus(String sql)
      throws SQLException {
    QueryHelper queryHelper = new QueryHelper(sql);
    List<CachedFileStatus> ret = new LinkedList<>();

    try {
      ResultSet resultSet = queryHelper.executeQuery();
      while (resultSet.next()) {
        CachedFileStatus f = new CachedFileStatus(
            resultSet.getLong("fid"),
            resultSet.getLong("from_time"),
            resultSet.getLong("last_access_time"),
            resultSet.getInt("num_accessed")
        );
        ret.add(f);
      }
      return ret.size() == 0 ? null : ret;
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
        + "checked_count, commands_generated"
        + (info.getLastCheckTime() == 0 ? "" : ", last_check_time")
        + ") VALUES ("
        + info.getState().getValue()
        + ", '" + info.getRuleText() + "'" // TODO: take care of '
        + ", " + info.getSubmitTime()
        + ", " + info.getNumChecked()
        + ", " + info.getNumCmdsGen()
        + (info.getLastCheckTime() == 0 ? "" : ", " + info.getLastCheckTime())
        +");";

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
      long lastCheckTime, long checkedCount, int commandsGen)
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
    if (commandsGen != 0) {
      sb.append(" commands_generated = commands_generated + ")
          .append(commandsGen).append(",");
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
            RuleState.fromValue((int)rs.getByte("state")),
            rs.getLong("checked_count"),
            rs.getLong("commands_generated"),
            rs.getLong("last_check_time")
        ));
      }
      return infos;
    } finally {
      queryHelper.close();
    }
  }

  public synchronized void insertCommandsTable(CommandInfo[] commands)
      throws SQLException {
    Connection conn = getConnection();
    Statement s = null;
    try {
      s = conn.createStatement();
      for (int i = 0; i < commands.length; i++) {
        String sql = "INSERT INTO commands (rid, action_id, state, "
            + "parameters, generate_time, state_changed_time) "
            + "VALUES('" + commands[i].getRid() + "', '"
            + commands[i].getActionType().getValue() + "', '"
            + commands[i].getState().getValue() + "', '"
            + commands[i].getParameters() + "', '"
            + commands[i].getGenerateTime() + "', '"
            + commands[i].getStateChangedTime() + "');";
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

  public synchronized boolean insertCommandTable(CommandInfo command)
      throws SQLException {
    // Insert single command into commands, update command.id with latest id
    String sql = "INSERT INTO commands (rid, action_id, state, "
            + "parameters, generate_time, state_changed_time) "
            + "VALUES('" + command.getRid() + "', '"
            + command.getActionType().getValue() + "', '"
            + command.getState().getValue() + "', '"
            + command.getParameters() + "', '"
            + command.getGenerateTime() + "', '"
            + command.getStateChangedTime() + "');";

    execute(sql);
    QueryHelper queryHelper = new QueryHelper("SELECT MAX(id) FROM commands;");
    try {
      ResultSet rs = queryHelper.executeQuery();
      if (rs.next()) {
        long cid = rs.getLong(1);
        command.setCid(cid);
        return true;
      } else {
        return false;
      }
    } finally {
      queryHelper.close();
    }
  }

  public List<CommandInfo> getCommandsTableItem(String cidCondition,
      String ridCondition, CommandState state) throws SQLException {
    String sqlPrefix = "SELECT * FROM commands WHERE ";
    String sqlCid = (cidCondition == null) ? "" : "AND cid " + cidCondition;
    String sqlRid = (ridCondition == null) ? "" : "AND rid " + ridCondition;
    String sqlState = (state == null) ? "" : "AND state = " + state.getValue();
    String sqlFinal = "";
    if (cidCondition != null || ridCondition != null || state != null) {
      sqlFinal = sqlPrefix + sqlCid + sqlRid + sqlState;
      sqlFinal = sqlFinal.replaceFirst("AND ", "");
      return getCommands(sqlFinal);
    }
    return null;
  }

  private List<CommandInfo> getCommands(String sql) throws SQLException {
    QueryHelper queryHelper = new QueryHelper(sql);
    try {
      ResultSet result = queryHelper.executeQuery();
      List<CommandInfo> ret = convertCommandsTableItem(result);
      return ret;
    } finally {
      queryHelper.close();
    }
  }

  public boolean updateCommandStatus(long cid, long rid, CommandState state)
      throws SQLException {
    StringBuffer sb = new StringBuffer("UPDATE commands SET");
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

  public void deleteCommand(long cid) throws SQLException {
    String sql =  String.format("DELETE from commands WHERE cid = %d;", cid);
    execute(sql);
  }

  private List<CommandInfo> convertCommandsTableItem(ResultSet resultSet)
      throws SQLException {
    List<CommandInfo> ret = new LinkedList<>();
    if (resultSet == null) {
      return ret;
    }

    while (resultSet.next()) {
      CommandInfo commands = new CommandInfo(
          resultSet.getLong("cid"),
          resultSet.getLong("rid"),
          ActionType.fromValue((int)resultSet.getByte("action_id")),
          CommandState.fromValue((int)resultSet.getByte("state")),
          resultSet.getString("parameters"),
          resultSet.getLong("generate_time"),
          resultSet.getLong("state_changed_time")
      );
      ret.add(commands);
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
      while(rs.next()) {
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
