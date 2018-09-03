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
package org.smartdata.server.engine.rule;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.exception.QueueFullException;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.metastore.dao.AccessCountTable;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.RuleState;
import org.smartdata.model.rule.RuleExecutorPlugin;
import org.smartdata.model.rule.RuleExecutorPluginManager;
import org.smartdata.model.rule.TimeBasedScheduleInfo;
import org.smartdata.model.rule.TranslateResult;
import org.smartdata.server.engine.RuleManager;
import org.smartdata.server.engine.data.ExecutionContext;

import java.io.IOException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Execute rule queries and return result. */
public class RuleExecutor implements Runnable {
  private RuleManager ruleManager;
  private TranslateResult tr;
  private ExecutionContext ctx;
  private MetaStore adapter;
  private volatile boolean exited = false;
  private long exitTime;
  private Stack<String> dynamicCleanups = new Stack<>();
  private static final Logger LOG = LoggerFactory.getLogger(RuleExecutor.class.getName());

  private static Pattern varPattern = Pattern.compile("\\$([a-zA-Z_]+[a-zA-Z0-9_]*)");
  private static Pattern callPattern =
      Pattern.compile("\\$@([a-zA-Z_]+[a-zA-Z0-9_]*)\\(([a-zA-Z_][a-zA-Z0-9_]*)?\\)");

  public RuleExecutor(
      RuleManager ruleManager, ExecutionContext ctx, TranslateResult tr, MetaStore adapter) {
    this.ruleManager = ruleManager;
    this.ctx = ctx;
    this.tr = tr;
    this.adapter = adapter;
  }

  public TranslateResult getTranslateResult() {
    return tr;
  }

  private String unfoldSqlStatement(String sql) {
    return unfoldVariables(unfoldFunctionCalls(sql));
  }

  private String unfoldVariables(String sql) {
    String ret = sql;
    ctx.setProperty("NOW", System.currentTimeMillis());
    Matcher m = varPattern.matcher(sql);
    while (m.find()) {
      String rep = m.group();
      String varName = m.group(1);
      String value = ctx.getString(varName);
      ret = ret.replace(rep, value);
    }
    return ret;
  }

  private String unfoldFunctionCalls(String sql) {
    String ret = sql;
    Matcher m = callPattern.matcher(sql);
    while (m.find()) {
      String rep = m.group();
      String funcName = m.group(1);
      String paraName = m.groupCount() == 2 ? m.group(2) : null;
      List<Object> params = tr.getParameter(paraName);
      String value = callFunction(funcName, params);
      ret = ret.replace(rep, value == null ? "" : value);
    }
    return ret;
  }

  public List<String> executeFileRuleQuery() {
    int index = 0;
    List<String> ret = new ArrayList<>();
    for (String sql : tr.getSqlStatements()) {
      sql = unfoldSqlStatement(sql);
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Rule " + ctx.getRuleId() + " --> " + sql);
        }
        if (index == tr.getRetSqlIndex()) {
          ret = adapter.executeFilesPathQuery(sql);
        } else {
          sql = sql.trim();
          if (sql.length() > 5) {
            adapter.execute(sql);
          }
        }
        index++;
      } catch (MetaStoreException e) {
        LOG.error("Rule " + ctx.getRuleId() + " exception", e);
        return ret;
      }
    }

    while (!dynamicCleanups.empty()) {
      String sql = dynamicCleanups.pop();
      try {
        adapter.execute(sql);
      } catch (MetaStoreException e) {
        LOG.error("Rule " + ctx.getRuleId() + " exception", e);
      }
    }
    return ret;
  }

  public String callFunction(String funcName, List<Object> parameters) {
    try {
      Method m = getClass().getMethod(funcName, List.class);
      String ret = (String) (m.invoke(this, parameters));
      return ret;
    } catch (Exception e) {
      LOG.error("Rule " + ctx.getRuleId() + " exception when call " + funcName, e);
      return null;
    }
  }

  public String genVirtualAccessCountTableTopValue(List<Object> parameters) {
    genVirtualAccessCountTableValue(parameters, true);
    return null;
  }

  public String genVirtualAccessCountTableBottomValue(List<Object> parameters) {
    genVirtualAccessCountTableValue(parameters, false);
    return null;
  }

  private void genVirtualAccessCountTableValue(List<Object> parameters, boolean top) {
    List<Object> paraList = (List<Object>) parameters.get(0);
    String table = (String) parameters.get(1);
    String var = (String) parameters.get(2);
    Long num = (Long) paraList.get(1);
    String sql0 = String.format(
        "SELECT %s(count) FROM ( SELECT * FROM %s ORDER BY count %sLIMIT %d ) AS %s_TMP;",
        top ? "min" : "max", table, top ? "DESC " : "", num, table);
    Long count = null;
    try {
      count = adapter.queryForLong(sql0);
    } catch (MetaStoreException e) {
      LOG.error("Get " + (top ? "top" : "bottom") + " access count from table '"
          + table + "' error.", e);
    }
    ctx.setProperty(var, count == null ? 0L : count);
  }

  public String genVirtualAccessCountTableTopValueOnStoragePolicy(List<Object> parameters) {
    genVirtualAccessCountTableValueOnStoragePolicy(parameters, true);
    return null;
  }

  public String genVirtualAccessCountTableBottomValueOnStoragePolicy(List<Object> parameters) {
    genVirtualAccessCountTableValueOnStoragePolicy(parameters, false);
    return null;
  }

  private void genVirtualAccessCountTableValueOnStoragePolicy(List<Object> parameters,
      boolean top) {
    List<Object> paraList = (List<Object>) parameters.get(0);
    String table = (String) parameters.get(1);
    String var = (String) parameters.get(2);
    Long num = (Long) paraList.get(1);
    String storage = ((String) paraList.get(2)).toUpperCase();
    String sqlsub;
    if (storage.equals("CACHE")) {
      sqlsub = String.format("SELECT %s.fid, %s.count FROM %s LEFT JOIN cached_file ON "
          + "(%s.fid = cached_file.fid)", table, table, table, table);
    } else {
      Integer id = null;
      try {
        id = adapter.getStoragePolicyID(storage);
      } catch (Exception e) {
        // Ignore
      }
      if (id == null) {
        id = -1; // safe return
      }
      sqlsub = String.format("SELECT %s.fid, %s.count FROM %s LEFT JOIN file ON "
          + "(%s.fid = file.fid) WHERE file.sid = %d",
          table, table, table, table, id);
    }

    String sql0 = String.format(
        "SELECT %s(count) FROM ( SELECT * FROM (%s) AS %s ORDER BY count %sLIMIT %d ) AS %s;",
        top ? "min" : "max",
        sqlsub,
        table + "_AL1_TMP",
        top ? "DESC " : "",
        num,
        table + "_AL2_TMP");
    Long count = null;
    try {
      count = adapter.queryForLong(sql0);
    } catch (MetaStoreException e) {
      LOG.error(String.format("Get %s access count on storage [%s] from table '%s' error [%s].",
          top ? "top" : "bottom", storage, table, sql0), e);
    }
    ctx.setProperty(var, count == null ? 0L : count);
  }

  public String genVirtualAccessCountTable(List<Object> parameters) {
    List<Object> paraList = (List<Object>) parameters.get(0);
    String newTable = (String) parameters.get(1);
    Long interval = (Long) paraList.get(0);
    String countFilter = "";
    List<String> tableNames = getAccessCountTablesDuringLast(interval);
    return generateSQL(tableNames, newTable, countFilter, adapter);
  }

  @VisibleForTesting
  static String generateSQL(
      List<String> tableNames, String newTable, String countFilter, MetaStore adapter) {
    String sqlFinal, sqlCreate;
    if (tableNames.size() <= 1) {
      String tableName = tableNames.size() == 0 ? "blank_access_count_info" : tableNames.get(0);
      sqlCreate = "CREATE TABLE " + newTable + "(fid INTEGER NOT NULL, count INTEGER NOT NULL);";
      try {
        adapter.execute(sqlCreate);
      } catch (MetaStoreException e) {
        LOG.error("Cannot create table " + newTable, e);
      }
      sqlFinal = "INSERT INTO " + newTable + " SELECT * FROM " + tableName + ";";
    } else {
      String sqlPrefix = "SELECT fid, SUM(count) AS count FROM (\n";
      String sqlUnion = "SELECT fid, count FROM " + tableNames.get(0) + " \n";
      for (int i = 1; i < tableNames.size(); i++) {
        sqlUnion += "UNION ALL\n" + "SELECT fid, count FROM " + tableNames.get(i) + " \n";
      }
      String sqlSufix = ") as tmp GROUP BY fid ";
      String sqlCountFilter =
          (countFilter == null || countFilter.length() == 0)
              ? ""
              : "HAVING SUM(count) " + countFilter;
      String sqlRe = sqlPrefix + sqlUnion + sqlSufix + sqlCountFilter;
      sqlCreate = "CREATE TABLE " + newTable + "(fid INTEGER NOT NULL, count INTEGER NOT NULL);";
      try {
        adapter.execute(sqlCreate);
      } catch (MetaStoreException e) {
        LOG.error("Cannot create table " + newTable, e);
      }
      sqlFinal = "INSERT INTO " + newTable + " SELECT * FROM (" + sqlRe + ") temp;";
    }
    return sqlFinal;
  }

  /**
   * @param lastInterval
   * @return
   */
  private List<String> getAccessCountTablesDuringLast(long lastInterval) {
    List<String> tableNames = new ArrayList<>();
    if (ruleManager == null || ruleManager.getStatesManager() == null) {
      return tableNames;
    }

    List<AccessCountTable> accTables = null;
    try {
      accTables = ruleManager.getStatesManager().getTablesInLast(lastInterval);
    } catch (MetaStoreException e) {
      LOG.error("Rule " + ctx.getRuleId() + " get access info tables exception", e);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Rule " + ctx.getRuleId() + " got " + accTables.size() + " tables:");
      int idx = 1;
      for (AccessCountTable t : accTables) {
        LOG.debug(
            idx + ".  " + (t.isEphemeral() ? " [TABLE] " : "        ") + t.getTableName() + " ");
      }
    }

    if (accTables == null || accTables.size() == 0) {
      return tableNames;
    }

    for (AccessCountTable t : accTables) {
      tableNames.add(t.getTableName());
      if (t.isEphemeral()) {
        dynamicCleanups.push("DROP TABLE IF EXISTS " + t.getTableName() + ";");
      }
    }
    return tableNames;
  }

  @Override
  public void run() {
    long startCheckTime = System.currentTimeMillis();
    if (exited) {
      exitSchedule();
    }

    if (!tr.getTbScheduleInfo().isExecutable(startCheckTime)) {
      return;
    }

    List<RuleExecutorPlugin> plugins = RuleExecutorPluginManager.getPlugins();

    long rid = ctx.getRuleId();
    try {
      if (ruleManager.isClosed()) {
        exitSchedule();
      }

      long endCheckTime;
      int numCmdSubmitted = 0;
      List<String> files = new ArrayList<>();

      RuleInfo info = ruleManager.getRuleInfo(rid);

      boolean doExec = true;
      for (RuleExecutorPlugin plugin : plugins) {
        doExec &= plugin.preExecution(info, tr);
        if (!doExec) {
          break;
        }
      }

      RuleState state = info.getState();
      if (exited
          || state == RuleState.DELETED
          || state == RuleState.FINISHED
          || state == RuleState.DISABLED) {
        exitSchedule();
      }
      TimeBasedScheduleInfo scheduleInfo = tr.getTbScheduleInfo();

      if (!scheduleInfo.isOnce() && scheduleInfo.getEndTime() != TimeBasedScheduleInfo.FOR_EVER) {
        boolean befExit = false;
        if (scheduleInfo.isOneShot()) {
          if (scheduleInfo.getSubScheduleTime() > scheduleInfo.getStartTime()) {
            befExit = true;
          }
        } else if (startCheckTime - scheduleInfo.getEndTime() > 0) {
          befExit = true;
        }

        if (befExit) {
          LOG.info("Rule " + ctx.getRuleId() + " exit rule executor due to time passed");
          ruleManager.updateRuleInfo(rid, RuleState.FINISHED, startCheckTime, 0, 0);
          exitSchedule();
        }
      }

      if (doExec) {
        files = executeFileRuleQuery();
        if (exited) {
          exitSchedule();
        }
      }
      endCheckTime = System.currentTimeMillis();
      if (doExec) {
        for (RuleExecutorPlugin plugin : plugins) {
          files = plugin.preSubmitCmdlet(info, files);
        }
        numCmdSubmitted = submitCmdlets(info, files);
      }
      ruleManager.updateRuleInfo(rid, null, startCheckTime, 1, numCmdSubmitted);

      long endProcessTime = System.currentTimeMillis();
      if (endProcessTime - startCheckTime > 2000 || LOG.isDebugEnabled()) {
        LOG.warn(
            "Rule "
                + ctx.getRuleId()
                + " execution took "
                + (endProcessTime - startCheckTime)
                + "ms. QueryTime = "
                + (endCheckTime - startCheckTime)
                + "ms, SubmitTime = "
                + (endProcessTime - endCheckTime)
                + "ms, fileNum = "
                + numCmdSubmitted
                + ".");
      }

      if (scheduleInfo.isOneShot()) {
        ruleManager.updateRuleInfo(rid, RuleState.FINISHED, startCheckTime, 0, 0);
        exitSchedule();
      }

      if (endProcessTime + scheduleInfo.getBaseEvery() > scheduleInfo.getEndTime()) {
        LOG.info("Rule " + ctx.getRuleId() + " exit rule executor due to finished");
        ruleManager.updateRuleInfo(rid, RuleState.FINISHED, startCheckTime, 0, 0);
        exitSchedule();
      }

      if (exited) {
        exitSchedule();
      }
    } catch (IOException e) {
      LOG.error("Rule " + ctx.getRuleId() + " exception", e);
    }
  }

  private void exitSchedule() {
    // throw an exception
    exitTime = System.currentTimeMillis();
    exited = true;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Rule " + ctx.getRuleId() + " exit rule executor.");
    }
    String[] temp = new String[1];
    temp[1] += "The exception is created deliberately";
  }

  private int submitCmdlets(RuleInfo ruleInfo, List<String> files) {
    long ruleId = ruleInfo.getId();
    if (files == null || files.size() == 0 || ruleManager.getCmdletManager() == null) {
      return 0;
    }
    int nSubmitted = 0;
    List<RuleExecutorPlugin> plugins = RuleExecutorPluginManager.getPlugins();
    String template = tr.getCmdDescriptor().toCmdletString();
    for (String file : files) {
      if (!exited) {
        try {
          CmdletDescriptor cmd = new CmdletDescriptor(template, ruleId);
          cmd.setCmdletParameter(CmdletDescriptor.HDFS_FILE_PATH, file);
          for (RuleExecutorPlugin plugin : plugins) {
            cmd = plugin.preSubmitCmdletDescriptor(ruleInfo, tr, cmd);
          }
          ruleManager.getCmdletManager().submitCmdlet(cmd);
          nSubmitted++;
        } catch (QueueFullException e) {
          break;
        } catch (IOException e) {
          // it's common here, ignore this and continue submit
          LOG.debug("Failed to submit cmdlet for file: " + file, e);
        } catch (ParseException e) {
          LOG.error("Failed to submit cmdlet for file: " + file, e);
        }
      } else {
        break;
      }
    }
    return nSubmitted;
  }

  public boolean isExited() {
    return exited;
  }

  public void setExited() {
    exitTime = System.currentTimeMillis();
    exited = true;
  }

  public long getExitTime() {
    return exitTime;
  }
}
