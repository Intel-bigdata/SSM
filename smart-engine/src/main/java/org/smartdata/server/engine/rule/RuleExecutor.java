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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.common.rule.RuleInfo;
import org.smartdata.common.rule.RuleState;
import org.smartdata.common.cmdlet.CmdletDescriptor;
import org.smartdata.server.engine.RuleManager;
import org.smartdata.server.engine.MetaStore;
import org.smartdata.server.engine.metastore.ExecutionContext;
import main.java.org.smartdata.metastore.tables.AccessCountTable;
import org.smartdata.rule.parser.TimeBasedScheduleInfo;
import org.smartdata.rule.parser.TranslateResult;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Execute rule queries and return result.
 */
public class RuleExecutor implements Runnable {
  private RuleManager ruleManager;
  private TranslateResult tr;
  private ExecutionContext ctx;
  private MetaStore adapter; // TODO: abstract to prevent direct call
  private volatile boolean exited = false;
  private long exitTime;
  private Stack<String> dynamicCleanups = new Stack<>();
  public static final Logger LOG =
      LoggerFactory.getLogger(RuleExecutor.class.getName());

  private static Pattern varPattern = Pattern.compile(
      "\\$([a-zA-Z_]+[a-zA-Z0-9_]*)");
  private static Pattern callPattern = Pattern.compile(
      "\\$@([a-zA-Z_]+[a-zA-Z0-9_]*)\\(([a-zA-Z_][a-zA-Z0-9_]*)?\\)");


  public RuleExecutor(RuleManager ruleManager, ExecutionContext ctx,
      TranslateResult tr, MetaStore adapter) {
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
      ret = ret.replace(rep, value);
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
          adapter.execute(sql);
        }
        index++;
      } catch (SQLException e) {
        LOG.error("Rule " + ctx.getRuleId() + " exception", e);
        return null;
      }
    }

    while (!dynamicCleanups.empty()) {
      String sql = dynamicCleanups.pop();
      try {
        adapter.execute(sql);
      } catch (SQLException e) {
        LOG.error("Rule " + ctx.getRuleId() + " exception", e);
      }
    }
    return ret;
  }

  public String callFunction(String funcName, List<Object> parameters) {
    try {
      Method m = getClass().getMethod(funcName, List.class);
      String ret = (String)(m.invoke(this, parameters));
      return ret;
    } catch (Exception e ) {
      LOG.error("Rule " + ctx.getRuleId()
          + " exception when call " + funcName, e);
      return null;
    }
  }

  private long timeNow() {
    Long p = ctx.getLong("Now");
    if (p == null) {
      return System.currentTimeMillis();
    }
    return p;
  }

  public String genVirtualAccessCountTable(List<Object> parameters) {
    List<Object> paraList = (List<Object>)parameters.get(0);
    String newTable = (String) parameters.get(1);
    Long interval = (Long)paraList.get(0);
    String countFilter = "";
    List<String> tableNames =
        getAccessCountTablesDuringLast(interval);

    String sqlFinal;
    if (tableNames.size() <= 1) {
      String tableName = tableNames.size() == 0 ? "blank_access_count_info" :
          tableNames.get(0);
      sqlFinal = "CREATE TABLE '" + newTable + "' AS SELECT * FROM '"
          + tableName + "';";
    } else {
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
      String sqlRe = sqlPrefix + sqlUnion + sqlSufix + sqlCountFilter;
      sqlFinal = "CREATE TABLE '" + newTable + "' AS SELECT * FROM ("
          + sqlRe + ");";
    }
    return sqlFinal;
  }

  /**
   *
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
    } catch (SQLException e) {
      LOG.error("Rule " + ctx.getRuleId()
          + " get access info tables exception", e);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Rule " + ctx.getRuleId() + " got "
          + accTables.size() + " tables:");
      int idx = 1;
      for (AccessCountTable t : accTables) {
        LOG.debug(idx + ".  " + (t.isView() ? " [VIEW] " : "        ")
            + t.getTableName() + " ");
      }
    }

    if (accTables == null || accTables.size() == 0) {
      return tableNames;
    }


    for (AccessCountTable t : accTables) {
      tableNames.add(t.getTableName());
      if (t.isView()) {
        dynamicCleanups.push("DROP VIEW " + t.getTableName() + ";");
      }
    }
    return tableNames;
  }

  /**
   * Get access count tables within the time interval.
   * @param startTime
   * @param endTime
   * @return can not be null
   */
  public static List<String> getAccessCountTablesBetween(
      long startTime, long endTime) {
    // TODO: hard code for test now
    return Arrays.asList("sec-2017-03-31-12-59-45", "sec-2017-03-31-12-59-50");
  }


  @Override
  public void run() {
    if (exited) {
      exitSchedule();
    }

    long rid = ctx.getRuleId();
    try {
      long startCheckTime = System.currentTimeMillis();
      if (ruleManager.isClosed()) {
        exitSchedule();
      }

      RuleInfo info = ruleManager.getRuleInfo(rid);
      RuleState state = info.getState();
      if (exited || state == RuleState.DELETED || state == RuleState.FINISHED
          || state == RuleState.DISABLED) {
        exitSchedule();
      }
      TimeBasedScheduleInfo scheduleInfo = tr.getTbScheduleInfo();

      if (scheduleInfo.getEndTime() != TimeBasedScheduleInfo.FOR_EVER
          // TODO: tricky here, time passed
          && startCheckTime - scheduleInfo.getEndTime() > 0) {
        // TODO: special for scheduleInfo.isOneShot()
        LOG.info("Rule " + ctx.getRuleId()
            + " exit rule executor due to time passed or finished");
        ruleManager.updateRuleInfo(rid, RuleState.FINISHED, timeNow(), 0, 0);
        exitSchedule();
      }


      List<String> files = executeFileRuleQuery();
      long endCheckTime = System.currentTimeMillis();
      int numCmdSubmitted = submitCmdlets(files, rid);
      ruleManager.updateRuleInfo(rid, null, timeNow(), 1, numCmdSubmitted);
      if (exited) {
        exitSchedule();
      }
      //System.out.println(this + " -> " + System.currentTimeMillis());
      long endProcessTime = System.currentTimeMillis();

      if (endProcessTime - startCheckTime > 3000 || LOG.isDebugEnabled()) {
        LOG.warn("Rule " + ctx.getRuleId() + " execution took "
            + (endProcessTime - startCheckTime) + "ms. QueryTime = "
            + (endCheckTime - startCheckTime) + "ms, SubmitTime = "
            + (endProcessTime - endCheckTime) + "ms.");
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

  private int submitCmdlets(List<String> files, long ruleId) {
    if (ruleManager.getCmdletExecutor() == null) {
      return 0;
    }
    int nSubmitted = 0;
    String template = tr.getCmdDescriptor().toCmdletString();
    for (String file : files) {
      if (!exited) {
        try {
          CmdletDescriptor cmd = new CmdletDescriptor(template, ruleId);
          cmd.setCmdletParameter(CmdletDescriptor.HDFS_FILE_PATH, file);
          ruleManager.getCmdletExecutor().submitCmdlet(cmd);
          nSubmitted++;
        } catch (IOException | ParseException e) {
          // ignore this and continue submit
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
