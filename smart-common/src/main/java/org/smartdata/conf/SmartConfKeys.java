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
  public static final String SMART_DFS_ENABLED = "smart.dfs.enabled";
  public static final boolean SMART_DFS_ENABLED_DEFAULT = true;

  public static final String SMART_CONF_DIR_KEY = "smart.conf.dir";
  public static final String SMART_HADOOP_CONF_DIR_KEY = "smart.hadoop.conf.path";
  public static final String SMART_CONF_DIR_DEFAULT = "conf";
  public static final String SMART_LOG_DIR_KEY = "smart.log.dir";
  public static final String SMART_LOG_DIR_DEFAULT = "logs";

  public static final String SMART_SERVICE_MODE_KEY = "smart.service.mode";
  public static final String SMART_SERVICE_MODE_DEFAULT = "HDFS";
  public static final String SMART_NAMESPACE_FETCHER_BATCH_KEY = "smart.namespace.fetcher.batch";
  public static final int SMART_NAMESPACE_FETCHER_BATCH_DEFAULT = 500;

  public static final String SMART_DFS_NAMENODE_RPCSERVER_KEY = "smart.dfs.namenode.rpcserver";

  // Configure keys for HDFS
  public static final String SMART_NAMESPACE_FETCHER_IGNORE_UNSUCCESSIVE_INOTIFY_EVENT_KEY =
      "smart.namespace.fetcher.ignore.unsuccessive.inotify.event";
  public static final boolean SMART_NAMESPACE_FETCHER_IGNORE_UNSUCCESSIVE_INOTIFY_EVENT_DEFAULT =
      false;
  public static final String SMART_NAMESPACE_FETCHER_PRODUCERS_NUM_KEY =
      "smart.namespace.fetcher.producers.num";
  public static final int SMART_NAMESPACE_FETCHER_PRODUCERS_NUM_DEFAULT = 3;
  public static final String SMART_NAMESPACE_FETCHER_CONSUMERS_NUM_KEY =
      "smart.namespace.fetcher.consumers.num";
  public static final int SMART_NAMESPACE_FETCHER_CONSUMERS_NUM_DEFAULT = 3;

  // Configure keys for Alluxio
  public static final String SMART_ALLUXIO_MASTER_HOSTNAME_KEY = "smart.alluxio.master.hostname";
  public static final String SMART_ALLUXIO_CONF_DIR_KEY = "smart.alluxio.conf.dir";
  public static final String SMART_ALLUXIO_MASTER_JOURNAL_DIR_KEY =
      "smart.alluxio.master.journal.dir";

  // SSM
  public static final String SMART_SERVER_RPC_ADDRESS_KEY = "smart.server.rpc.address";
  public static final String SMART_SERVER_RPC_ADDRESS_DEFAULT = "0.0.0.0:7042";
  public static final String SMART_SERVER_RPC_HANDLER_COUNT_KEY = "smart.server.rpc.handler.count";
  public static final int SMART_SERVER_RPC_HANDLER_COUNT_DEFAULT = 80;
  public static final String SMART_SERVER_HTTP_ADDRESS_KEY = "smart.server.http.address";
  public static final String SMART_SERVER_HTTP_ADDRESS_DEFAULT = "0.0.0.0:7045";
  public static final String SMART_SERVER_HTTPS_ADDRESS_KEY = "smart.server.https.address";
  public static final String SMART_SECURITY_ENABLE = "smart.security.enable";
  public static final String SMART_SERVER_KEYTAB_FILE_KEY = "smart.server.keytab.file";
  public static final String SMART_SERVER_KERBEROS_PRINCIPAL_KEY =
    "smart.server.kerberos.principal";
  public static final String SMART_AGENT_KEYTAB_FILE_KEY = "smart.agent.keytab.file";
  public static final String SMART_AGENT_KERBEROS_PRINCIPAL_KEY =
    "smart.agent.kerberos.principal";
  public static final String SMART_SECURITY_CLIENT_PROTOCOL_ACL =
    "smart.security.client.protocol.acl";
  public static final String SMART_SECURITY_ADMIN_PROTOCOL_ACL =
    "smart.security.admin.protocol.acl";
  public static final String SMART_METASTORE_DB_URL_KEY = "smart.metastore.db.url";

  // StatesManager

  // RuleManager
  public static final String SMART_RULE_EXECUTORS_KEY = "smart.rule.executors";
  public static final int SMART_RULE_EXECUTORS_DEFAULT = 5;

  public static final String SMART_CMDLET_EXECUTORS_KEY = "smart.cmdlet.executors";
  public static final int SMART_CMDLET_EXECUTORS_DEFAULT = 10;
  public static final String SMART_DISPATCH_CMDLETS_EXTRA_NUM_KEY =
      "smart.dispatch.cmdlets.extra.num";
  public static final int SMART_DISPATCH_CMDLETS_EXTRA_NUM_DEFAULT = 10;

  // Keep it only for test
  public static final String SMART_ENABLE_ZEPPELIN_WEB = "smart.zeppelin.web.enable";
  public static final boolean SMART_ENABLE_ZEPPELIN_WEB_DEFAULT = true;

  // Cmdlets
  public static final String SMART_CMDLET_MAX_NUM_PENDING_KEY =
      "smart.cmdlet.max.num.pending";
  public static final int SMART_CMDLET_MAX_NUM_PENDING_DEFAULT = 20000;
  public static final String SMART_CMDLET_HIST_MAX_NUM_RECORDS_KEY =
      "smart.cmdlet.hist.max.num.records";
  public static final int SMART_CMDLET_HIST_MAX_NUM_RECORDS_DEFAULT =
      100000;
  public static final String SMART_CMDLET_HIST_MAX_RECORD_LIFETIME_KEY =
      "smart.cmdlet.hist.max.record.lifetime";
  public static final String SMART_CMDLET_HIST_MAX_RECORD_LIFETIME_DEFAULT =
      "30day";
  public static final String SMART_CMDLET_CACHE_BATCH =
      "smart.cmdlet.cache.batch";
  public static final int SMART_CMDLET_CACHE_BATCH_DEFAULT =
      600;
  public static final String SMART_CMDLET_MOVER_MAX_CONCURRENT_BLOCKS_PER_SRV_INST_KEY =
      "smart.cmdlet.mover.max.concurrent.blocks.per.srv.inst";
  public static final int SMART_CMDLET_MOVER_MAX_CONCURRENT_BLOCKS_PER_SRV_INST_DEFAULT = 0;

  // Schedulers
  public static final String SMART_COPY_SCHEDULER_BASE_SYNC_BATCH =
      "smart.copy.scheduler.base.sync.batch";
  public static final int SMART_COPY_SCHEDULER_BASE_SYNC_BATCH_DEFAULT =
      500;
  public static final String SMART_COPY_SCHEDULER_CHECK_INTERVAL =
      "smart.copy.scheduler.check.interval";
  public static final int SMART_COPY_SCHEDULER_CHECK_INTERVAL_DEFAULT =
      500;
  public static final String SMART_FILE_DIFF_MAX_NUM_RECORDS_KEY =
      "smart.file.diff.max.num.records";
  public static final int SMART_FILE_DIFF_MAX_NUM_RECORDS_DEFAULT =
      10000;

  // Dispatcher
  public static final String SMART_CMDLET_DISPATCHER_LOG_DISP_RESULT_KEY =
      "smart.cmdlet.dispatcher.log.disp.result";
  public static final boolean SMART_CMDLET_DISPATCHER_LOG_DISP_RESULT_DEFAULT = false;
  public static final String SMART_CMDLET_DISPATCHERS_KEY = "smart.cmdlet.dispatchers";
  public static final int SMART_CMDLET_DISPATCHERS_DEFAULT = 3;
  public static final String SMART_CMDLET_DISPATCHER_LOG_DISP_METRICS_INTERVAL_KEY =
      "smart.cmdlet.dispatcher.log.disp.metrics.interval"; // in ms
  public static final int SMART_CMDLET_DISPATCHER_LOG_DISP_METRICS_INTERVAL_DEFAULT = 5000;

  // Action
  public static final String SMART_ACTION_MOVE_THROTTLE_MB_KEY = "smart.action.move.throttle.mb";
  public static final long SMART_ACTION_MOVE_THROTTLE_MB_DEFAULT = 0L;  // 0 means unlimited
  public static final String SMART_ACTION_COPY_THROTTLE_MB_KEY = "smart.action.copy.throttle.mb";
  public static final long SMART_ACTION_COPY_THROTTLE_MB_DEFAULT = 0L;  // 0 means unlimited
  public static final String SMART_ACTION_EC_THROTTLE_MB_KEY = "smart.action.ec.throttle.mb";
  public static final long SMART_ACTION_EC_THROTTLE_MB_DEFAULT = 0L;
  public static final String SMART_ACTION_LOCAL_EXECUTION_DISABLED_KEY =
    "smart.action.local.execution.disabled";
  public static final boolean SMART_ACTION_LOCAL_EXECUTION_DISABLED_DEFAULT = false;

  // SmartAgent
  public static final String SMART_AGENT_MASTER_PORT_KEY = "smart.agent.master.port";
  public static final int SMART_AGENT_MASTER_PORT_DEFAULT = 7051;
  public static final String SMART_AGENT_PORT_KEY = "smart.agent.port";
  public static final int SMART_AGENT_PORT_DEFAULT = 7048;

  /** Do NOT configure the following two options manually. They are set by the boot scripts. **/
  public static final String SMART_AGENT_MASTER_ADDRESS_KEY = "smart.agent.master.address";
  public static final String SMART_AGENT_ADDRESS_KEY = "smart.agent.address";

  // Small File Compact
  public static final String SMART_COMPACT_BATCH_SIZE_KEY =
      "smart.compact.batch.size";
  public static final int SMART_COMPACT_BATCH_SIZE_DEFAULT =
      200;
  public static final String SMART_COMPACT_CONTAINER_FILE_THRESHOLD_MB_KEY =
      "smart.compact.container.file.threshold.mb";
  public static final long SMART_COMPACT_CONTAINER_FILE_THRESHOLD_MB_DEFAULT =
      1024;

  // SmartClient

  // Common
  /**
   * Namespace, access info and other info related to files under these dirs will be ignored.
   * Clients will not report access event of these files to SSM.
   * For more than one directories, they should be separated by ",".
   */
  public static final String SMART_IGNORE_DIRS_KEY = "smart.ignore.dirs";
  /**
   * Namespace, access info and other info only related to files under these dirs will be tackled.
   * For more than one directories, they should be separated by ",".
   */
  public static final String SMART_COVER_DIRS_KEY = "smart.cover.dirs";
  public static final String SMART_TMP_DIR_KEY = "smart.tmp.dir";
  public static final String SMART_TMP_DIR_DEFAULT = "/system/ssm/";

  // Target cluster
  public static final String SMART_STORAGE_INFO_UPDATE_INTERVAL_KEY =
      "smart.storage.info.update.interval";
  public static final int SMART_STORAGE_INFO_UPDATE_INTERVAL_DEFAULT = 60;
  public static final String SMART_STORAGE_INFO_SAMPLING_INTERVALS_KEY =
      "smart.storage.info.sampling.intervals";
  public static final String SMART_STORAGE_INFO_SAMPLING_INTERVALS_DEFAULT =
      "60s,60;1hour,60;1day";
  public static final String SMART_TOP_HOT_FILES_NUM_KEY = "smart.top.hot.files.num";
  public static final int SMART_TOP_HOT_FILES_NUM_DEFAULT = 200;

  //Status report
  public static final String SMART_STATUS_REPORT_PERIOD_KEY = "smart.status.report.period";
  public static final int SMART_STATUS_REPORT_PERIOD_DEFAULT = 10;
  public static final String SMART_STATUS_REPORT_PERIOD_MULTIPLIER_KEY =
      "smart.status.report.period.multiplier";
  public static final int SMART_STATUS_REPORT_PERIOD_MULTIPLIER_DEFAULT = 50;
  public static final String SMART_STATUS_REPORT_RATIO_KEY = "smart.status.report.ratio";
  public static final double SMART_STATUS_REPORT_RATIO_DEFAULT = 0.2;

  // Compression
  public static final String SMART_COMPRESSION_IMPL = "smart.compression.impl";
  public static final String SMART_COMPRESSION_IMPL_DEFAULT = "Zlib";
  public static final String SMART_COMPRESSION_BUFFER_SIZE = "smart.compression.buffer.size";
  public static final int SMART_COMPRESSION_BUFFER_SIZE_DEFAULT = 262144;
  public static final String SMART_COMPRESSION_MAX_SPLIT = "smart.compression.max.split";
  public static final int SMART_COMPRESSION_MAX_SPLIT_DEFAULT = 1000;
}
