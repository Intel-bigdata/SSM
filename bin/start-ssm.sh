#!/usr/bin/env bash
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

bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "${bin}">/dev/null; pwd)
HOSTNAME=$(hostname)

SMART_VARGS=
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
      DEBUG_OPT=$1
      shift
      ;;
    *)
      SMART_VARGS+=" $1"
      shift
      ;;
  esac
done

. "${bin}/common.sh"
get_smart_servers

#---------------------------------------------------------
# Start Smart Servers
SERVERS_IN_HAZELCAST=$("${SMART_HOME}/bin/ssm" getconf SmartServers 2>/dev/null)
if [ "$SERVERS_IN_HAZELCAST" != "" ]; then
  echo "ERROR: Get SmartServers error. Please don't add member in hazelcast.xml"
  exit 1
fi

SMART_VARGS_STANDBY=
for i in $SMART_VARGS; do
  if [ "$i" != "-format" ]; then
    SMART_VARGS_STANDBY+=" $i"
  fi
done

if [ x"${SMARTSERVERS}" != x"" ]; then
  echo "Starting SmartServers on [${SMARTSERVERS}]"
  FIRST_MASTER=$(echo ${SMARTSERVERS} | awk '{print $1}')
  . "${SMART_HOME}/bin/ssm" \
    --remote \
    --config "${SMART_CONF_DIR}" \
    --hosts "${FIRST_MASTER}" --hostsend \
    --daemon start ${DEBUG_OPT} \
    smartserver $SMART_VARGS

  if [ x"${SMARTSERVERS}" != x"${FIRST_MASTER}" ]; then
    OTHER_MASTERS=${SMARTSERVERS/${FIRST_MASTER} /}
    sleep 4
    . "${SMART_HOME}/bin/ssm" \
      --remote \
      --config "${SMART_CONF_DIR}" \
      --hosts "${OTHER_MASTERS}" --hostsend \
      --daemon start ${DEBUG_OPT} \
      smartserver $SMART_VARGS_STANDBY
  fi
else
  echo "ERROR: No SmartServers configured in 'servers'."
  exit 1
fi

echo

#---------------------------------------------------------
# Start Smart Agents
AGENT_MASTER=${SMARTSERVERS// /,}

AGENTS_FILE="${SMART_CONF_DIR}/agents"
if [ -f "${AGENTS_FILE}" ]; then
  AGENT_HOSTS=$(sed 's/#.*$//;/^$/d' "${AGENTS_FILE}" | xargs echo)

  if [ x"${AGENT_HOSTS}" != x"" ]; then
    . "${SMART_HOME}"/bin/start-agent.sh \
      --host "${AGENT_HOSTS}" \
      --debug "${DEBUG_OPT}"
  fi
fi
