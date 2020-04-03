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
# Description: Start standby Smart Server
#

bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "${bin}">/dev/null; pwd)

STANDBY_HOSTS=
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
      STANDBY_HOSTS="$1"
      shift
      ;;
    "--debug")
      DEBUG_OPT_STANDBY="$1"
      shift
      ;;
    "--help" | "-h")
      echo "--help -h Show this usage information"
      echo "--config Specify or overwrite an configure option."
      echo "--host Specify the host on which standby Smart Server will be started by providing its hostname or IP."
      echo "  The default one is localhost."
      shift
      ;;
    *)
      break;
      ;;
  esac
done

# Executing start-standby-server.sh with no host option will start a standby server on localhost.
if [[ -z "${STANDBY_HOSTS}" ]]; then
  STANDBY_HOSTS=localhost
fi

. "${bin}/common.sh"
get_smart_servers

AGENT_MASTER=${SMARTSERVERS// /,}

AH=
for i in $STANDBY_HOSTS; do if [ "$i" = "localhost" ]; then AH+=" ${HOSTNAME}" ; else AH+=" $i"; fi; done
  STANDBY_HOSTS=${AH/ /}

echo "Starting standby Smart Server on [${STANDBY_HOSTS}]"
. "${SMART_HOME}/bin/ssm" \
  --remote \
  --config "${SMART_CONF_DIR}" \
  --hosts "${STANDBY_HOSTS}" --hostsend \
  --daemon start ${DEBUG_OPT_STANDBY} \
  standby -D smart.agent.master.address=${AGENT_MASTER}
