#!/usr/bin/env bash
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

export SMART_SERVER_LOG_FILE_NAME=smartserver.log
export SMART_AGENT_LOG_FILE_NAME=smartagent.log
export SMART_LOG_FILE_NAME=${SMART_SERVER_LOG_FILE_NAME}

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
export SMART_LOG_FILE=${SMART_LOG_DIR}/${SMART_LOG_FILE_NAME}

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

function get_smart_servers(){
  ORGSMARTSERVERS=
  export SERVERS_FILE="${SMART_CONF_DIR}/servers"
  if [ -f "${SERVERS_FILE}" ]; then
    ORGSMARTSERVERS=$(sed 's/#.*$//;/^$/d' "${SERVERS_FILE}" | xargs echo)
    if [ "$?" != "0" ]; then
      echo "ERROR: Get SmartServers error."
      exit 1
    fi

    CONTAIN_LOCALHOST=
    HH=
    for i in $ORGSMARTSERVERS; do if [ "$i" = "localhost" ]; then HH+=" ${HOSTNAME}" ; CONTAIN_LOCALHOST=true ; else HH+=" $i"; fi; done
    export SMARTSERVERS=${HH/ /}

    if [ x"${CONTAIN_LOCALHOST}" = x"true" -a x"${ORGSMARTSERVERS}" != x"localhost" ]; then
        echo "ERROR: 'localhost' cannot be used when starting multiple SmartServers."
        echo "       Please replace it with the real hostname in servers."
        exit 1
    fi
  else
    echo "${SERVERS_FILE} doesn't exist!"
    exit 1
  fi
}

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

SSH_OPTIONS="-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10s"

function check_java_avaliable() {
  "${SMART_RUNNER}" -version >/dev/null 2>&1
  if [[ $? -ne 0 ]]; then
    echo "#===================================================================="
    echo "#  Cannot find java. Please config JAVA_HOME in conf/smart-env.sh"
    echo "#===================================================================="
    return 1;
  fi
  return 0;
}

function start_smart_server() {
  echo "Starting SmartServer ..."
  smart_start_daemon ${SMART_PID_FILE}
}

function stop_smart_server() {
  local servers=localhost
  smart_stop_daemon ${SMART_PID_FILE}
}

function smart_start_daemon() {
  local pidfile=$1

  if [[ -f "${pidfile}" ]]; then
    pid=$(cat "$pidfile")
    if ps -p "${pid}" > /dev/null 2>&1; then
      echo "ERROR: Another instance is running, please stop it first."
      return 1
    fi
    rm -f "${pidfile}" >/dev/null 2>&1
  fi

  start_daemon "${pidfile}" >>${SMART_LOG_FILE} 2>&1 < /dev/null &

  (( counter=0 ))
  while [[ ! -f ${pidfile} && ${counter} -le 5 ]]; do
    sleep 1
    (( counter++ ))
  done

  echo $! > "${pidfile}" 2>/dev/null
  if [[ $? -gt 0 ]]; then
    echo "ERROR:  Can NOT write pid file ${pidfile}."
  fi

  disown %+ >/dev/null 2>&1
  if [[ $? -gt 0 ]]; then
    echo "ERROR: Cannot disconnect process $!"
  fi
  sleep 1

  if ! ps -p $! >/dev/null 2>&1; then
    return 1
  fi
  return 0
}

function start_daemon() {
  local pidfile=$1

  echo $$ > "${pidfile}" 2>/dev/null
  if [[ $? -gt 0 ]]; then
    echo "ERROR: Can NOT write PID file ${pidfile}."
  fi

  exec $SMART_RUNNER $JAVA_OPTS -cp "${SMART_CLASSPATH}" $SMART_CLASSNAME $SMART_VARGS
}

function smart_stop_daemon() {
  local pidfile=$1
  shift

  local pid
  local cur_pid

  if [[ -f "${pidfile}" ]]; then
    pid=$(cat "$pidfile")

    kill "${pid}" >/dev/null 2>&1
    (( counter=0 ))
    while [[ ${counter} -le 5 ]]; do
      sleep 1
      (( counter++ ))
      ps -p "${pid}" > /dev/null 2>&1
      if [ "$?" != "0" ]; then
        echo "Service stopped on node '${HOSTNAME}'"
        break
      fi
    done

    if kill -0 "${pid}" > /dev/null 2>&1; then
      echo "Daemon still alive after 5 seconds, Trying to kill it by force."
      kill -9 "${pid}" >/dev/null 2>&1
      sleep 1
    fi
    if ps -p "${pid}" > /dev/null 2>&1; then
      echo "ERROR: Unable to kill ${pid}"
    fi
    rm -f "$pidfile"
  else
    echo "Service not found on node '${HOSTNAME}'"
  fi
}

function init_command() {
  local subcmd=$1
  shift

  case ${subcmd} in
    formatdatabase)
      SMART_CLASSNAME=org.smartdata.server.SmartDaemon
      SMART_PID_FILE=/tmp/SmartServer.pid
      ALLOW_DAEMON_OPT=true
      SMART_VARGS+=" -format"
      JAVA_OPTS+=" -Dsmart.log.file="${SMART_LOG_FILE_NAME}
    ;;
    smartserver)
      SMART_CLASSNAME=org.smartdata.server.SmartDaemon
      SMART_PID_FILE=/tmp/SmartServer.pid
      ALLOW_DAEMON_OPT=true
      JAVA_OPTS+=" -Dsmart.log.file="${SMART_LOG_FILE_NAME}
      SMART_VARGS+=" -D smart.agent.master.address="${SSM_EXEC_HOST}
    ;;
    smartagent)
      SMART_CLASSNAME=org.smartdata.agent.SmartAgent
      SMART_PID_FILE=/tmp/SmartAgent.pid
      ALLOW_DAEMON_OPT=true
      export SMART_LOG_FILE_NAME=${SMART_AGENT_LOG_FILE_NAME}
      export SMART_LOG_FILE=${SMART_LOG_DIR}/${SMART_LOG_FILE_NAME}
      JAVA_OPTS+=" -Dsmart.log.file="${SMART_LOG_FILE_NAME}
      SMART_VARGS+=" -D smart.agent.address="${SSM_EXEC_HOST}
    ;;
    getconf)
      SMART_CLASSNAME=org.smartdata.server.utils.tools.GetConf
    ;;
    *)
      echo "Unkown command ${subcmd}"
      exit 1;
    ;;
  esac
}

function remote_execute() {
  local host=$1
  shift

  ssh ${SSH_OPTIONS} ${host} "export SSM_EXEC_HOST=${host}; cd ${SMART_HOME} ; $@"
}

function local_execute() {
  exec $SMART_RUNNER $JAVA_OPTS -cp "${SMART_CLASSPATH}" $SMART_CLASSNAME $SMART_VARGS
}
