#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Run SmartServer
#
#./bin/start-smart.sh -D smart.dfs.namenode.rpcserver=hdfs://localhost:9000
#./bin/start-smart.sh -D smart.dfs.namenode.rpcserver=hdfs://localhost:9000 -D dfs.smart.default.db.url=jdbc:sqlite:file-sql.db

USAGE="Usage: bin/start-smart.sh [--config <conf-dir>] [--debug] ..."

bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "${bin}">/dev/null; pwd)

echo "Command: $0 $*"

vargs=
while [ $# != 0 ]; do
  case "$1" in
    "--config")
      shift
      conf_dir="$1"
      if [[ ! -d "${conf_dir}" ]]; then
        echo "ERROR : ${conf_dir} is not a directory"
        echo ${USAGE}
        exit 1
      else
        export SMART_CONF_DIR="${conf_dir}"
        echo "SMART_CONF_DIR="$SMART_CONF_DIR
      fi
      shift
      ;;
    "--debug")
      JAVA_OPTS+=" -Xdebug -Xrunjdwp:transport=dt_socket,address=8008,server=y,suspend=y"
      shift
      ;;
    *)
      vargs+=" $1"
      shift
      ;;
  esac
done

. "${bin}/common.sh"


HOSTNAME=$(hostname)

SMART_SERVER=org.smartdata.server.SmartDaemon
JAVA_OPTS+=" -Dsmart.log.dir=${SMART_LOG_DIR}"

addJarInDir "${SMART_HOME}/smart-server/target/lib"
addNonTestJarInDir "${SMART_HOME}/smart-server/target"
addJarInDir "${SMART_HOME}/lib"

if [ "$SMART_CLASSPATH" = "" ]; then
  SMART_CLASSPATH="${SMART_CONF_DIR}"
else
  SMART_CLASSPATH="${SMART_CONF_DIR}:${SMART_CLASSPATH}"
fi

if [[ ! -d "${SMART_LOG_DIR}" ]]; then
  echo "Log dir doesn't exist, create ${SMART_LOG_DIR}"
  $(mkdir -p "${SMART_LOG_DIR}")
fi

vargs+=" -D smart.conf.dir="${SMART_CONF_DIR}
vargs+=" -D smart.log.dir="${SMART_LOG_DIR}
exec $SMART_RUNNER $JAVA_OPTS -cp "${SMART_CLASSPATH}" $SMART_SERVER $vargs
