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

import org.apache.hadoop.ssm.rule.parser.TranslateResult;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Execute rule queries and return result.
 */
public class RuleQueryExecutor {
  private TranslateResult tr;
  private ExecutionContext ctx;
  private DBAdapter adapter;

  private static Pattern varPattern = Pattern.compile(
      "\\$([a-zA-Z_]+[a-zA-Z0-9_]*)");
  private static Pattern callPattern = Pattern.compile(
      "\\$@([a-zA-Z_]+[a-zA-Z0-9_]*)\\(([a-zA-Z_][a-zA-Z0-9_]*)?\\)");


  public RuleQueryExecutor(ExecutionContext ctx, TranslateResult tr,
      DBAdapter adapter) {
    this.ctx = ctx;
    this.tr = tr;
    this.adapter = adapter;
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
        //System.out.println("--> " + sql);
        if (index == tr.getRetSqlIndex()) {
          ret = adapter.executeFilesPathQuery(sql);
        } else {
          adapter.execute(sql);
        }
        index++;
      } catch (SQLException e) {
        e.printStackTrace();
        return null;
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
    long now = timeNow();
    String countFilter = "";
    List<String> tableNames =
        getAccessCountTablesBetween(now - interval, now);
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
    String sqlFinal = "CREATE TABLE '" + newTable + "' AS SELECT * FROM ("
        + sqlRe + ")";
    return sqlFinal;
  }

  /**
   * Get access count tables within the time interval.
   * @param startTime
   * @param endTime
   * @return
   */
  public static List<String> getAccessCountTablesBetween(
      long startTime, long endTime) {
    // TODO: hard code for test now
    return Arrays.asList("sec-2017-03-31-12-59-45", "sec-2017-03-31-12-59-50");
  }
}
