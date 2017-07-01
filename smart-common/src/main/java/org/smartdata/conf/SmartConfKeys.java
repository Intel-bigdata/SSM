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
package org.smartdata.conf;

/**
 * This class contains the configure keys needed by SSM.
 */
public class SmartConfKeys {
  public final static String SMART_DFS_ENABLED = "smart.dfs.enabled";
  public final static boolean SMART_DFS_ENABLED_DEFAULT = true;

  public final static String SMART_DFS_NAMENODE_RPCSERVER_KEY = "smart.dfs.namenode.rpcserver";

  //ssm
  public final static String SMART_SERVER_RPC_ADDRESS_KEY = "smart.server.rpc.address";
  public final static String SMART_SERVER_RPC_ADDRESS_DEFAULT = "0.0.0.0:7042";
  public final static String SMART_SERVER_HTTP_ADDRESS_KEY = "smart.server.http.address";
  public final static String SMART_SERVER_HTTP_ADDRESS_DEFAULT = "0.0.0.0:7045";
  public final static String SMART_SERVER_HTTPS_ADDRESS_KEY = "smart.server.https.address";
  public final static String SMART_SECURITY_ENABLE = "smart.security.enable";
  public final static String SMART_SERVER_KEYTAB_FILE_KEY = "smart.server.keytab.file";
  public final static String SMART_SERVER_KERBEROS_PRINCIPAL_KEY = "smart.server.kerberos.principal";
  public final static String SMART_METASTORE_DB_URL_KEY = "smart.metastore.db.url";

  // StatesManager
  public static final String SMART_STATES_UPDATE_SERVICE_KEY = "smart.states.update.service";
  public static final String SMART_STATES_UPDATE_SERVICE_DEFAULT = "org.smartdata.hdfs.HdfsStatesUpdaterService";

  // RuleManager
  public final static String SMART_RULE_EXECUTORS_KEY = "smart.rule.executors";
  public final static int SMART_RULE_EXECUTORS_DEFAULT = 5;

  public final static String SMART_CMDLET_EXECUTORS_KEY = "smart.cmdlet.executors";
  public final static int SMART_CMDLET_EXECUTORS_DEFAULT = 10;

  public final static String SMART_ENABLE_ZEPPELIN_WEB = "smart.zeppelin.web.enable";
  public final static boolean SMART_ENABLE_ZEPPELIN_WEB_DEFAULT = false;
}
