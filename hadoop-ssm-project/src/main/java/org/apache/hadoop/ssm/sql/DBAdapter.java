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
package org.apache.hadoop.ssm.sql;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.ssm.CommandState;
import org.apache.hadoop.ssm.actions.ActionType;
import org.apache.hadoop.ssm.rule.RuleInfo;
import org.apache.hadoop.ssm.rule.RuleState;

import java.sql.Connection;
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
    String sqlGetTableNames = "SELECT table_name FROM access_count_tables " +
        "WHERE start_time >= " + startTime + " AND end_time <= " + endTime;
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
        String sql = "INSERT INTO 'files' VALUES('" + files[i].getPath() +
            "','" + files[i].getFileId() + "','" + files[i].getLen() + "','" +
            files[i].getReplication() + "','" + files[i].getBlockSize() + "','" +
            files[i].getModificationTime() + "','" + files[i].getAccessTime() +
            "','" + booleanToInt(files[i].isDir()) + "','" + files[i].getStoragePolicy() +
            "','" + getKey(mapOwnerIdName, files[i].getOwner()) + "','" +
            getKey(mapGroupIdName, files[i].getGroup()) + "','" +
            files[i].getPermission().toShort() + "','" +
            getKey(mapECPolicy, files[i].getErasureCodingPolicy()) + "');";
        s.addBatch(sql);
      }
      s.executeBatch();
    }catch (SQLException e) {
        e.printStackTrace();
    }
  }

  public int booleanToInt(boolean b) {
    if (b) {
      return 1;
    } else {
      return 0;
    }
  }

  public Integer getKey(Map<Integer, String> map, String value) {
    for (Integer key: map.keySet()) {
      if (map.get(key).equals(value)) {
        return key;
      }
    }
    return null;
  }
  public Integer getKey(Map<Integer, ErasureCodingPolicy> map, ErasureCodingPolicy value) {
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
    return mapECPolicy.get(id) != null ? mapECPolicy.get(id) : null;
  }

  public StorageCapacity getStorageCapacity(String type) {
    updateCache();
    return mapStorageCapacity.get(type) != null ?
        mapStorageCapacity.get(type) : null;
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

  private Map<String, StorageCapacity> convertStorageTablesItem(ResultSet resultSet) {
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
  }

  public synchronized void insertCachedFiles(List<CachedFileStatus> s) {
  }

  public synchronized void updateCachedFiles(long fid,
      long lastAccessTime, int numAccessed) {
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
    } catch (SQLException e) {
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
    ResultSet result = s.executeQuery(sqlQuery);
    return result;
  }

  public int executeUpdate(String sqlUpdate) throws SQLException {
    Statement s = conn.createStatement();
    int result = s.executeUpdate(sqlUpdate);
    return result;
  }

  public void execute(String sql) throws SQLException {
    Statement s = conn.createStatement();
    s.executeUpdate(sql);
  }

  public List<String> executeFilesPathQuery(String sql) throws SQLException {
    List<String> paths = new LinkedList<>();
    ResultSet res = executeQuery(sql);
    while (res.next()) {
      paths.add(res.getString(1));
    }
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
        + ", " + info.getCountConditionChecked()
        + ", " + info.getCountConditionFulfilled()
        + (info.getLastCheckTime() == 0 ? "" : ", " + info.getLastCheckTime())
        +");";

    try {
      execute(sql);
      ResultSet rs = executeQuery("SELECT MAX(id) FROM rules;");
      if (rs.next()) {
        ruleId = rs.getLong(1);
        info.setId(ruleId);
        return true;
      } else {
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
        String sql = "INSERT INTO commands(rid, action_id, state, "
            + "parameters, generate_time, state_changed_time) "
            + "VALUES('" + commands[i].getRid() + "', '"
            + commands[i].getActionId().getValue() + "', '"
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

  public List<CommandInfo> getCommandsTableItem(String cidCondition,
      String ridCondition, CommandState state) {
    String sqlPrefix = "SELECT * FROM commands WHERE ";
    String sqlCid = (cidCondition == null) ? "" : "AND cid " + cidCondition;
    String sqlRid = (ridCondition == null) ? "" : "AND rid " + ridCondition;
    String sqlState = (state == null) ? "" : "AND state = " + state;
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
    if (resultSet != null) {
      try {
        resultSet.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return ret;
  }
}
