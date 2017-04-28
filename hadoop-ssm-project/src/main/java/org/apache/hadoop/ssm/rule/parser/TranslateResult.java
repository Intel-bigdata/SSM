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
package org.apache.hadoop.ssm.rule.parser;

import java.util.List;
import java.util.Map;

/**
 * Result of rule translation. A guide for execution.
 */
public class TranslateResult {
  private List<String> retColumns;
  private int retSqlIndex;
  private List<String> staticTempTables; // to be deleted after execution
  List<String> sqlStatements;
  Map<String, List<Object>> dynamicParameters;

  public TranslateResult(List<String> sqlStatements,
      List<String> tempTableNames, Map<String, List<Object>> dynamicParameters,
       int retSqlIndex) {
    this.sqlStatements = sqlStatements;
    this.staticTempTables = tempTableNames;
    this.dynamicParameters = dynamicParameters;
    this.retSqlIndex = retSqlIndex;
  }

  public void setSqlStatements(List<String> sqlStatements) {
    this.sqlStatements = sqlStatements;
  }

  public List<String> getSqlStatements() {
    return sqlStatements;
  }

  public List<String> getStaticTempTables() {
    return staticTempTables;
  }

  public List<Object> getParameter(String paramName) {
    return dynamicParameters.get(paramName);
  }

  public int getRetSqlIndex() {
    return retSqlIndex;
  }
}
