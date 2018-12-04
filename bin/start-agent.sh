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
# Description: Start Smart Agent
#

bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "${bin}">/dev/null; pwd)

AGENT_HOSTS=
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
    "--host")
      shift
      AGENT_HOSTS="$1"
      shift
      ;;
    "--debug")
      DEBUG_OPT_AGENT="$1"
      shift
      ;;
    "--help" | "-h")
      echo -e "--help -h Show this usage information\n\
--config Specify or overwrite an configure option.\n\
--host Specify the host on which Smart Agent will be started by giving its hostname or IP. \
The default one is localhost. "
      shift
      ;;
    "--help" | "-h")
      echo -e "--help -h Show this usage information\n--config Specify or overwrite an configure option.\n--host Specify the host on which Smart Agent will be started by giving its hostname or IP. The default one is localhost. "
      shift
      ;;
    *)
      break;
      ;;
  esac
done

#Running start-agent.sh with no host option will start an agent on localhost
if [[ -z "${AGENT_HOSTS}" ]]; then
  AGENT_HOSTS=localhost
fi

. "${bin}/common.sh"
get_smart_servers

AGENT_MASTER=${SMARTSERVERS// /,}

AH=
for i in $AGENT_HOSTS; do if [ "$i" = "localhost" ]; then AH+=" ${HOSTNAME}" ; else AH+=" $i"; fi; done
  AGENT_HOSTS=${AH/ /}

echo "Starting SmartAgents on [${AGENT_HOSTS}]"
. "${SMART_HOME}/bin/ssm" \
  --remote \
  --config "${SMART_CONF_DIR}" \
  --hosts "${AGENT_HOSTS}" --hostsend \
  --daemon start ${DEBUG_OPT_AGENT} \
  smartagent -D smart.agent.master.address=${AGENT_MASTER}
