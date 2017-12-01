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
    *)
      SMART_VARGS+=" $1"
      shift
      ;;
  esac
done

. "${bin}/common.sh"

#---------------------------------------------------------
# Stop Smart Servers

ORGSMARTSERVERS=
SERVERS_FILE="${SMART_CONF_DIR}/servers"
if [ -f "${SERVERS_FILE}" ]; then
  ORGSMARTSERVERS=$(sed 's/#.*$//;/^$/d' "${SERVERS_FILE}" | xargs echo)
  if [ "$?" != "0" ]; then
    echo "ERROR: Get SmartServers error."
    exit 1
  fi

  HH=
  for i in $ORGSMARTSERVERS; do if [ "$i" = "localhost" ]; then HH+=" ${HOSTNAME}" ; else HH+=" $i"; fi; done
  SMARTSERVERS=${HH/ /}

  if [ x"${SMARTSERVERS}" != x"" ]; then
    echo "Stopping SmartServers on [${SMARTSERVERS}]"
    . "${SMART_HOME}/bin/ssm" \
      --remote \
      --config "${SMART_CONF_DIR}" \
      --hosts "${SMARTSERVERS}" --hostsend \
      --daemon stop ${DEBUG_OPT} \
      smartserver
  else
    echo "No SmartServers configured in 'servers'."
  fi

  echo
fi
#---------------------------------------------------------
# Stop Smart Agents

AGENTS_FILE="${SMART_CONF_DIR}/agents"
if [ -f "${AGENTS_FILE}" ]; then
  AGENT_HOSTS=$(sed 's/#.*$//;/^$/d' "${AGENTS_FILE}" | xargs echo)
  AH=
  for i in $AGENT_HOSTS; do if [ "$i" = "localhost" ]; then AH+=" ${HOSTNAME}" ; else AH+=" $i"; fi; done
  AGENT_HOSTS=${AH/ /}
  if [ x"${AGENT_HOSTS}" != x"" ]; then
    echo "Stopping SmartAgents on [${AGENT_HOSTS}]"
    . "${SMART_HOME}/bin/ssm" \
      --remote \
      --config "${SMART_CONF_DIR}" \
      --hosts "${AGENT_HOSTS}" --hostsend \
      --daemon stop ${DEBUG_OPT} \
      smartagent
  fi
fi
