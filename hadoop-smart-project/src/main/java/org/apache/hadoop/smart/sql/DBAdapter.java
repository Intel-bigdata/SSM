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
package org.apache.hadoop.smart.sql;

import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.smart.CommandState;
import org.apache.hadoop.smart.actions.ActionType;
import org.apache.hadoop.smart.rule.RuleInfo;
import org.apache.hadoop.smart.rule.RuleState;
import org.apache.hadoop.smart.sql.tables.AccessCountTable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Operations supported for upper functions.
 */
public class DBAdapter {

  private Connection conn;

  private Map<Integer, String> mapOwnerIdName = null;
  private Map<Integer, String> mapGroupIdName = null;
  private Map<Integer, String> mapStoragePolicyIdName = null;
  private Map<Integer, ErasureCodingPolicy> mapECPolicy = null;
  private Map<String, StorageCapacity> mapStorageCapacity = null;

  public DBAdapter(Connection conn) {
    this.conn = conn;
  }

  public Map<Long, Integer> getAccessCount(long startTime, long endTime,
      String countFilter) {
    String sqlGetTableNames = "SELECT table_name FROM access_count_tables "
        + "WHERE start_time >= " + startTime + " AND end_time <= " + endTime;
    try {
      ResultSet rsTableNames = executeQuery(sqlGetTableNames);
      List<String> tableNames = new LinkedList<>();
      while (rsTableNames.next()) {
        tableNames.add(rsTableNames.getString(1));
      }
      if (rsTableNames != null) {
        rsTableNames.close();
      }
      if (tableNames.size() == 0) {
        return null;
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

      ResultSet rsValues = executeQuery(sqlFinal);

      Map<Long, Integer> ret = new HashMap<>();
      while (rsValues.next()) {
        ret.put(rsValues.getLong(1), rsValues.getInt(2));
      }
      if(rsValues != null) {
        rsValues.close();
      }
      return ret;
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Store access count data for the given time interval.
   *
   * @param startTime
   * @param endTime
   * @param fids
   * @param counts
   */
  public synchronized void insertAccessCountData(long startTime, long endTime,
      long[] fids, int[] counts) {
  }

  /**
   * Store files info into database.
   *
   * @param files
   */
  public synchronized void insertFiles(FileStatusInternal[] files) {
    updateCache();
    try {
      Statement s = conn.createStatement();
      for (int i = 0; i < files.length; i++) {
        String sql = "INSERT INTO `files` (path, fid, length, "
            + "block_replication,"
            + " block_size, modification_time, access_time, is_dir, sid, oid, "
            + "gid, permission, ec_policy_id ) "
            + "VALUES ('" + files[i].getPath()
            + "','" + files[i].getFileId() + "','" + files[i].getLen() + "','"
            + files[i].getReplication() + "','" + files[i].getBlockSize()
            + "','" + files[i].getModificationTime() + "','"
            + files[i].getAccessTime()
            + "','" + booleanToInt(files[i].isDir()) + "','"
            + files[i].getStoragePolicy() + "','"
            + getKey(mapOwnerIdName, files[i].getOwner()) + "','"
            + getKey(mapGroupIdName, files[i].getGroup()) + "','"
            + files[i].getPermission().toShort() + "','"
            + getKey(mapECPolicy, files[i].getErasureCodingPolicy()) + "');";
        s.addBatch(sql);
      }
      s.executeBatch();
    }catch (SQLException e) {
        e.printStackTrace();
    }
  }

  private int booleanToInt(boolean b) {
    if (b) {
      return 1;
    } else {
      return 0;
    }
  }

  private Integer getKey(Map<Integer, String> map, String value) {
    for (Integer key: map.keySet()) {
      if (map.get(key).equals(value)) {
        return key;
      }
    }
    return null;
  }

  private Integer getKey(Map<Integer, ErasureCodingPolicy> map,
      ErasureCodingPolicy value) {
    for (Integer key : map.keySet()) {
      if (map.get(key).equals(value)) {
        return key;
      }
    }
    return null;
  }

  public HdfsFileStatus getFile(long fid) {
    String sql = "SELECT * FROM files WHERE fid = " + fid;
    ResultSet result;
    try {
      result = executeQuery(sql);
    } catch (SQLException e) {
      return null;
    }
    List<HdfsFileStatus> ret = convertFilesTableItem(result);
    if (result != null) {
      try {
        result.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return ret.size() > 0 ? ret.get(0) : null;
  }

  public Map<String, Long> getFileIDs(Collection<String> paths) {
    Map<String, Long> pathToId = new HashMap<>();
    String in = paths.stream().map(s -> "'" + s +"'")
        .collect(Collectors.joining(", "));
    String sql = "SELECT fid, path FROM files WHERE path IN (" + in + ")";
    ResultSet result;
    try {
      result = executeQuery(sql);
      while (result.next()) {
        pathToId.put(result.getString("path"),
            result.getLong("fid"));
      }
    } catch (SQLException e) {
      e.printStackTrace();
      return pathToId;
    }
    return pathToId;
  }

  public HdfsFileStatus getFile(String path) {
    String sql = "SELECT * FROM files WHERE path = \'" + path + "\'";
    ResultSet result;
    try {
      result = executeQuery(sql);
    } catch (SQLException e) {
      return null;
    }
    List<HdfsFileStatus> ret = convertFilesTableItem(result);
    if (result != null) {
      try {
        result.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return ret.size() > 0 ? ret.get(0) : null;
  }

  public ErasureCodingPolicy getErasureCodingPolicy(int id) {
    updateCache();
    return mapECPolicy.get(id);
  }

  public synchronized void insertStoragesTable(StorageCapacity[] storages) {
    try {
      Statement s = conn.createStatement();
      for (int i = 0; i < storages.length; i++) {
        String sql = "INSERT INTO `storages` (type, capacity, free) VALUES"
            + " ('" + storages[i].getType()
            + "','" + storages[i].getCapacity() + "','"
            + storages[i].getFree() + "')";
        s.addBatch(sql);
      }
      s.executeBatch();
      mapStorageCapacity = null;
    }catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public StorageCapacity getStorageCapacity(String type) {
    updateCache();
    return mapStorageCapacity.get(type);
  }

  public synchronized boolean updateStoragesTable(String type,
      Long capacity, Long free) {
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
    try {
      mapStorageCapacity = null;
      return executeUpdate(sql) == 1;
    } catch (SQLException e) {
      return false;
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
  public List<HdfsFileStatus> convertFilesTableItem(ResultSet resultSet) {
    List<HdfsFileStatus> ret = new LinkedList<>();
    if (resultSet == null) {
      return ret;
    }
    updateCache();
    try {
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
            resultSet.getByte("sid"),
            mapECPolicy.get(resultSet.getShort("ec_policy_id")));
        ret.add(status);
      }
    } catch (SQLException e) {
      return null;
    }
    if (resultSet != null) {
      try {
        resultSet.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return ret;
  }

  private void updateCache() {
    try {
      if (mapOwnerIdName == null) {
        String sql = "SELECT * FROM owners";
        mapOwnerIdName = convertToMap(executeQuery(sql));
      }

      if (mapGroupIdName == null) {
        String sql = "SELECT * FROM groups";
        mapGroupIdName = convertToMap(executeQuery(sql));
      }

      if (mapStoragePolicyIdName == null) {
        String sql = "SELECT * FROM storage_policy";
        mapStoragePolicyIdName = convertToMap(executeQuery(sql));
      }

      if (mapECPolicy == null) {
        String sql = "SELECT * FROM ecpolicys";
        mapECPolicy = convertEcPoliciesTableItem(executeQuery(sql));
      }
      if (mapStorageCapacity == null) {
        String sql = "SELECT * FROM storages";
        mapStorageCapacity = convertStorageTablesItem(executeQuery(sql));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private Map<Integer, ErasureCodingPolicy> convertEcPoliciesTableItem(
      ResultSet resultSet) {
    Map<Integer, ErasureCodingPolicy> ret = new HashMap<>();
    if (resultSet == null) {
      return ret;
    }
    try {
      while (resultSet.next()) {
        int id = resultSet.getInt("id");

        ECSchema schema = new ECSchema(
            resultSet.getString("codecName"),
            resultSet.getInt("numDataUnits"),
            resultSet.getInt("numParityUnits")
        );

        ErasureCodingPolicy ec = new ErasureCodingPolicy(
            resultSet.getString("name"),
            schema,
            resultSet.getInt("cellsize"),
            (byte)id
        );

        ret.put(id, ec);
      }
    } catch (SQLException e) {
      return null;
    }
    return ret;
  }

  private Map<String, StorageCapacity> convertStorageTablesItem(
      ResultSet resultSet) {
    Map<String, StorageCapacity> map = new HashMap<>();
    if (resultSet == null) {
      return map;
    }
    try {
      while (resultSet.next()) {
        String type = resultSet.getString(1);
        StorageCapacity storage = new StorageCapacity(
            resultSet.getString(1),
            resultSet.getLong(2),
            resultSet.getLong(3));
        map.put(type, storage);
      }
    } catch (SQLException e) {
      return null;
    }
    return map;
  }

  private Map<Integer, String> convertToMap(ResultSet resultSet) {
    Map<Integer, String> ret = new HashMap<>();
    if (resultSet == null) {
      return ret;
    }
    try {
      while (resultSet.next()) {
        // TODO: Tests for this
        ret.put(resultSet.getInt(1), resultSet.getString(2));
      }
    } catch (SQLException e) {
      return null;
    }
    return ret;
  }

  public synchronized void insertCachedFiles(long fid, long fromTime,
      long lastAccessTime, int numAccessed) {
    try {
      String sql = "INSERT INTO cached_files (fid, from_time, "
          + "last_access_time, num_accessed) VALUES (" + fid + ","
          + fromTime + "," + lastAccessTime + ","
          + numAccessed + ")";
      executeUpdate(sql);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public synchronized void insertCachedFiles(List<CachedFileStatus> s) {
    try {
      Statement st = conn.createStatement();
      for (CachedFileStatus c : s) {
        String sql = "INSERT INTO cached_files (fid, from_time, "
            + "last_access_time, num_accessed) VALUES (" + c.getFid() + ","
            + c.getFromTime() + "," + c.getLastAccessTime() + ","
            + c.getNumAccessed() + ")";
        st.addBatch(sql);
      }
      st.executeBatch();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public synchronized boolean updateCachedFiles(Long fid, Long fromTime,
      Long lastAccessTime, Integer numAccessed) {
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
    try {
      return executeUpdate(sb.toString()) == 1;
    } catch (SQLException e) {
      return false;
    }
  }

  public List<CachedFileStatus> getCachedFileStatus() {
    String sql = "SELECT * FROM cached_files";
    return getCachedFileStatus(sql);
  }

  public CachedFileStatus getCachedFileStatus(long fid) {
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
    this.execute(sql);
  }

  private List<CachedFileStatus> getCachedFileStatus(String sql) {
    ResultSet resultSet;
    List<CachedFileStatus> ret = new LinkedList<>();

    try {
      resultSet = executeQuery(sql);

      while (resultSet.next()) {
        CachedFileStatus f = new CachedFileStatus(
            resultSet.getLong("fid"),
            resultSet.getLong("from_time"),
            resultSet.getLong("last_access_time"),
            resultSet.getInt("num_accessed")
        );
        ret.add(f);
      }
      resultSet.close();
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }
    if(resultSet != null) {
      try {
        resultSet.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return ret.size() == 0 ? null : ret;
  }

  public ResultSet executeQuery(String sqlQuery) throws SQLException {
    Statement s = conn.createStatement();
    return s.executeQuery(sqlQuery);
  }

  public int executeUpdate(String sqlUpdate) throws SQLException {
    Statement s = conn.createStatement();
    return s.executeUpdate(sqlUpdate);
  }

  public void execute(String sql) throws SQLException {
    Statement s = conn.createStatement();
    s.execute(sql);
  }

  //Todo: optimize
  public void execute(List<String> statements) throws SQLException {
    for (String statement : statements) {
      this.execute(statement);
    }
  }

  public List<String> executeFilesPathQuery(String sql) throws SQLException {
    List<String> paths = new LinkedList<>();
    ResultSet res = executeQuery(sql);
    while (res.next()) {
      paths.add(res.getString(1));
    }
    res.close();
    return paths;
  }

  public synchronized void close() {
  }

  public List<HdfsFileStatus> executeFileRuleQuery() {
    ResultSet resultSet = null;
    return convertFilesTableItem(resultSet);
  }

  public synchronized boolean insertNewRule(RuleInfo info) {
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

    try {
      execute(sql);
      ResultSet rs = executeQuery("SELECT MAX(id) FROM rules;");
      if (rs.next()) {
        ruleId = rs.getLong(1);
        info.setId(ruleId);
        rs.close();
        return true;
      } else {
        rs.close();
        return false;
      }
    } catch (SQLException e) {
    }
    return false;
  }

  public synchronized boolean updateRuleInfo(long ruleId, RuleState rs, long lastCheckTime,
      long checkedCount, int commandsGen) {
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
    sb.replace(idx, idx, "");
    sb.append(" WHERE id = ").append(ruleId).append(";");
    try {
      return 1 == executeUpdate(sb.toString());
    } catch (SQLException e) {
      return false;
    }
  }

  public RuleInfo getRuleInfo(long ruleId) {
    String sql = "SELECT * FROM rules WHERE id = " + ruleId;
    List<RuleInfo> infos = doGetRuleInfo(sql);
    return infos.size() == 1 ? infos.get(0) : null;
  }

  public List<RuleInfo> getRuleInfo() {
    String sql = "SELECT * FROM rules";
    return doGetRuleInfo(sql);
  }

  private List<RuleInfo> doGetRuleInfo(String sql) {
    List<RuleInfo> infos = new LinkedList<>();
    try {
      ResultSet rs = executeQuery(sql);
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
    } catch (SQLException e) {
    }
    return infos;
  }

  public synchronized void insertCommandsTable(CommandInfo[] commands) {
    try {
      Statement s = conn.createStatement();
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
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public synchronized boolean insertCommandTable(CommandInfo command) {
    // Insert single command into commands, update command.id with latest id
    String sql = "INSERT INTO commands (rid, action_id, state, "
            + "parameters, generate_time, state_changed_time) "
            + "VALUES('" + command.getRid() + "', '"
            + command.getActionType().getValue() + "', '"
            + command.getState().getValue() + "', '"
            + command.getParameters() + "', '"
            + command.getGenerateTime() + "', '"
            + command.getStateChangedTime() + "');";
    try {
      execute(sql);
      ResultSet rs = executeQuery("SELECT MAX(id) FROM commands;");
      if (rs.next()) {
        long cid = rs.getLong(1);
        command.setCid(cid);
        rs.close();
        return true;
      } else {
        rs.close();
        return false;
      }
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
  }

  public List<CommandInfo> getCommandsTableItem(String cidCondition,
      String ridCondition, CommandState state) {
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

  private List<CommandInfo> getCommands(String sql) {
    ResultSet result;
    try {
      result = executeQuery(sql);
    } catch (SQLException e) {
      return null;
    }
    List<CommandInfo> ret = convertCommandsTableItem(result);
    if (result != null) {
      try {
        result.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return ret;
  }

  public boolean updateCommandStatus(long cid, long rid, CommandState state) {
    StringBuffer sb = new StringBuffer("UPDATE commands SET");
    if (state != null) {
      sb.append(" state = ").append(state.getValue()).append(",");
      sb.append(" state_changed_time = ").append(System.currentTimeMillis()).append(",");
    }
    int idx = sb.lastIndexOf(",");
    sb.replace(idx, idx + 1, "");
    sb.append(" WHERE cid = ").append(cid).append(" AND ").append("rid = ").append(rid).append(";");
    try {
      return executeUpdate(sb.toString()) == 1;
    } catch (SQLException e) {
      return false;
    }
  }

  // TODO multiple CommandStatus update
//  public boolean updateCommandsStatus(Map<Long, CommandState> cmdMap) {
//    try {
//      Statement s = conn.createStatement();
//      for(Map.Entry<Long, CommandState> entry: cmdMap.entrySet()) {
//        StringBuffer sb = new StringBuffer("UPDATE commands SET");
//        if (entry.getValue() != null) {
//          sb.append(" state = ").append(entry.getValue().getValue()).append(",");
//          sb.append(" state_changed_time = ").append(System.currentTimeMillis()).append(",");
//        }
//        int idx = sb.lastIndexOf(",");
//        sb.replace(idx, idx + 1, "");
//        sb.append(" WHERE cid = ").append(entry.getKey()).append(";");
//        s.addBatch(sb.toString());
//      }
//      s.executeBatch();
//    } catch (SQLException e) {
//      e.printStackTrace();
//    }
//    return true;
//  }

  private List<CommandInfo> convertCommandsTableItem(ResultSet resultSet) {
    List<CommandInfo> ret = new LinkedList<>();
    if (resultSet == null) {
      return ret;
    }
    try {
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
    } catch (SQLException e) {
      return null;
    }
    try {
      resultSet.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return ret;
  }

  public synchronized void insertStoragePolicyTable(StoragePolicy s) {
  String sql = "INSERT INTO `storage_policy` (sid, policy_name) VALUES('"
      + s.getSid() + "','" + s.getPolicyName() + "');";
    try {
      execute(sql);
      mapStoragePolicyIdName = null;
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public String getStoragePolicyName(int sid) {
    updateCache();
    return mapStoragePolicyIdName.get(sid);
  }

  public Integer getStoragePolicyID(String policyName) {
    updateCache();
    return getKey(mapStoragePolicyIdName, policyName);
  }

  public synchronized boolean insertXattrTable(Long fid, Map<String, byte[]> map) {
    String sql = "INSERT INTO xattr (fid, namespace, name, value) "
        + "VALUES (?, ?, ?, ?)";
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
      conn.commit();
      conn.setAutoCommit(true);
      if (i.length == map.size()) {
        return true;
      } else {
        return false;
      }
    } catch (SQLException e) {
      return false;
    }
  }

  public Map<String, byte[]> getXattrTable(Long fid) {
    String sql =
        String.format("SELECT * FROM xattr WHERE fid = %s;", fid);
    return getXattrTable(sql);
  }

  private Map<String, byte[]> getXattrTable(String sql) {
    ResultSet rs;
    List<XAttr> list = new LinkedList<>();
    try {
      rs = executeQuery(sql);
      while(rs.next()) {
        XAttr xAttr = new XAttr.Builder().setNameSpace(
            XAttr.NameSpace.valueOf(rs.getString("namespace")))
            .setName(rs.getString("name"))
            .setValue(rs.getBytes("value")).build();
        list.add(xAttr);
      }
      return XAttrHelper.buildXAttrMap(list);
    } catch (SQLException e) {
      return null;
    }
  }
}
