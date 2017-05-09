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
package org.apache.hadoop.ssm;

/**
 * This class contains the configure keys needed by SSM.
 */
public class SSMConfigureKeys {
  public final static String DFS_SSM_ENABLED_KEY = "dfs.ssm.enabled";

  public final static String DFS_SSM_NAMENODE_RPCSERVER_KEY = "dfs.ssm.namenode.rpcserver";

  //ssm
  public final static String DFS_SSM_RPC_ADDRESS_KEY = "dfs.ssm.rpc-address";
  public final static String DFS_SSM_RPC_ADDRESS_DEFAULT = "localhost:7042";
  public final static String DFS_SSM_HTTP_ADDRESS_KEY = "dfs.ssm.http-address";
  public final static String DFS_SSM_HTTP_ADDRESS_DEFAULT = "localhost:7045";
  public final static String DFS_SSM_HTTPS_ADDRESS_KEY = "dfs.ssm.https-address";

  public final static String DFS_SSM_DEFAULT_DB_URL_KEY = "dfs.ssm.default.db.url";
}
