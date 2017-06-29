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
# Run SmartAgent

USAGE="Usage: bin/start-agent.sh [--config <conf-dir>] ..."

bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "${bin}">/dev/null; pwd)

echo "Command: $0 $*"

if [[ "$1" == "--config" ]]; then
  shift
  conf_dir="$1"
  if [[ ! -d "${conf_dir}" ]]; then
    echo "ERROR : ${conf_dir} is not a directory"
    echo ${USAGE}
    exit 1
  else
    export SMART_CONF_DIR="${conf_dir}"
  fi
  shift
fi

. "${bin}/common.sh"

HOSTNAME=$(hostname)
SMART_LOGFILE="${SMART_LOG_DIR}/smart-${SMART_IDENT_STRING}-${HOSTNAME}.log"
LOG="${SMART_LOG_DIR}/smart-cli-${SMART_IDENT_STRING}-${HOSTNAME}.out"

SMART_AGENT=org.smartdata.agent.SmartAgent

addJarInDir "${SMART_HOME}/lib"

if [[ ! -d "${SMART_LOG_DIR}" ]]; then
  echo "Log dir doesn't exist, create ${SMART_LOG_DIR}"
  $(mkdir -p "${SMART_LOG_DIR}")
fi

# if [[ ! -d "${SMART_PID_DIR}" ]]; then
#   echo "Pid dir doesn't exist, create ${SMART_PID_DIR}"
#   $(mkdir -p "${SMART_PID_DIR}")
# fi

exec $SMART_RUNNER $JAVA_OPTS -cp "${SMART_CLASSPATH}" $SMART_AGENT "$@"