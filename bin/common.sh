#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

if [ -L ${BASH_SOURCE-$0} ]; then
  FWDIR=$(dirname $(readlink "${BASH_SOURCE-$0}"))
else
  FWDIR=$(dirname "${BASH_SOURCE-$0}")
fi

if [[ -z "${SMART_HOME}" ]]; then
  # Make SMART_HOME look cleaner in logs by getting rid of the
  # extra ../
  export SMART_HOME="$(cd "${FWDIR}/.."; pwd)"
fi

if [[ -z "${SMART_CONF_DIR}" ]]; then
  export SMART_CONF_DIR="${SMART_HOME}/conf"
fi

if [[ -z "${SMART_LOG_DIR}" ]]; then
  export SMART_LOG_DIR="${SMART_HOME}/logs"
fi

if [[ -z "$SMART_PID_DIR" ]]; then
  export SMART_PID_DIR="${SMART_HOME}/run"
fi

if [[ -f "${SMART_CONF_DIR}/smart-env.sh" ]]; then
  . "${SMART_CONF_DIR}/smart-env.sh"
fi

if [ "$SMART_CLASSPATH" = "" ]; then
  SMART_CLASSPATH="${SMART_CONF_DIR}"
else
  SMART_CLASSPATH+=":${SMART_CONF_DIR}"
fi

function addNonTestJarInDir(){
  if [[ -d "${1}" ]]; then
    for jar in $(find -L "${1}" -maxdepth 1 -name '*jar' | grep -v '\-tests.jar'); do
      SMART_CLASSPATH="$jar:$SMART_CLASSPATH"
    done
  fi
}

function addEachJarInDir(){
  if [[ -d "${1}" ]]; then
    for jar in $(find -L "${1}" -maxdepth 1 -name '*jar'); do
      SMART_CLASSPATH="$jar:$SMART_CLASSPATH"
    done
  fi
}

function addEachJarInDirRecursive(){
  if [[ -d "${1}" ]]; then
    for jar in $(find -L "${1}" -type f -name '*jar'); do
      SMART_CLASSPATH="$jar:$SMART_CLASSPATH"
    done
  fi
}

function addEachJarInDirRecursiveForIntp(){
  if [[ -d "${1}" ]]; then
    for jar in $(find -L "${1}" -type f -name '*jar'); do
      SMART_INTP_CLASSPATH="$jar:$SMART_INTP_CLASSPATH"
    done
  fi
}

function addJarInDir(){
  if [[ -d "${1}" ]]; then
    SMART_CLASSPATH="${1}/*:${SMART_CLASSPATH}"
  fi
}

function addJarInDirForIntp() {
  if [[ -d "${1}" ]]; then
    SMART_INTP_CLASSPATH="${1}/*:${SMART_INTP_CLASSPATH}"
  fi
}

# Text encoding for 
# read/write job into files,
# receiving/displaying query/result.
if [[ -z "${SMART_ENCODING}" ]]; then
  export SMART_ENCODING="UTF-8"
fi

# if [[ -z "${SMART_MEM}" ]]; then
#   export SMART_MEM="-Xms1024m -Xmx1024m -XX:MaxPermSize=512m"
# fi

# if [[ -z "${SMART_INTP_MEM}" ]]; then
#   export SMART_INTP_MEM="-Xms1024m -Xmx1024m -XX:MaxPermSize=512m"
# fi

# JAVA_OPTS+=" ${SMART_JAVA_OPTS} -Dfile.encoding=${SMART_ENCODING} ${SMART_MEM}"
# JAVA_OPTS+=" -Dlog4j.configuration=file://${SMART_CONF_DIR}/log4j.properties"
export JAVA_OPTS

JAVA_INTP_OPTS="${SMART_INTP_JAVA_OPTS} -Dfile.encoding=${SMART_ENCODING}"
JAVA_INTP_OPTS+=" -Dlog4j.configuration=file://${SMART_CONF_DIR}/log4j.properties"
export JAVA_INTP_OPTS


if [[ -n "${JAVA_HOME}" ]]; then
  SMART_RUNNER="${JAVA_HOME}/bin/java"
else
  SMART_RUNNER=java
fi
export SMART_RUNNER

if [[ -z "$SMART_IDENT_STRING" ]]; then
  export SMART_IDENT_STRING="${USER}"
fi

if [[ -z "$SMART_INTERPRETER_REMOTE_RUNNER" ]]; then
  export SMART_INTERPRETER_REMOTE_RUNNER="bin/interpreter.sh"
fi


SMART_SERVER_PID_FILE=/tmp/SmartServer.pid

SSH_OPTIONS="-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10s"

function start_smart_server() {
  local servers=localhost
  ssh ${SSH_OPTIONS} ${servers} cd ${SMART_HOME}; ${SMART_HOME}/bin/start-smart.sh "--daemon" 2>&1 >/dev/null &
}

function stop_smart_server() {
  local servers=localhost
  ssh ${SSH_OPTIONS} ${servers} ${SMART_HOME}/bin/stop-smart.sh "--daemon" 2>&1 >/dev/null &
}

function smart_start_daemon() {
  local pidfile=$1

  if [[ -f "${pidfile}" ]]; then
    pid=$(cat "$pidfile")
    if ps -p "${pid}" > /dev/null 2>&1; then
      return
    fi
  fi

  echo $$ > "${pidfile}" 2>/dev/null
  if [[ $? -gt 0 ]]; then
    echo "ERROR: Can NOT write PID file ${pidfile}."
  fi

  exec $SMART_RUNNER $JAVA_OPTS -cp "${SMART_CLASSPATH}" $SMART_SERVER $SMART_VARGS
}

function smart_stop_daemon() {
  local pidfile=$1
  shift

  local pid
  local cur_pid

  if [[ -f "${pidfile}" ]]; then
    pid=$(cat "$pidfile")

    kill "${pid}" >/dev/null 2>&1
    sleep 5
    if kill -0 "${pid}" > /dev/null 2>&1; then
      echo "Daemon still alive after 5 seconds, Trying to kill it by force."
      kill -9 "${pid}" >/dev/null 2>&1
    fi
    if ps -p "${pid}" > /dev/null 2>&1; then
      echo "ERROR: Unable to kill ${pid}"
    fi
    rm -f "$pidfile"
  else
    echo "Can NOT find PID file $pidfile"
  fi
}