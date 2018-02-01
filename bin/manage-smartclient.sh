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

if [ $# = 0 ]; then
    echo " [--hosts <host names>] [--hostsfile <hosts file path>]"
    exit 0
fi

# Host names string
function parse_hosts_string() {
  local ORGHOSTSTR=$*
  local CONTAIN_LOCALHOST=
  local HH=
  for i in $ORGHOSTSTR; do if [ "$i" = "localhost" ]; then HH+=" ${HOSTNAME}" ; CONTAIN_LOCALHOST=true ; else HH+=" $i"; fi; done
  export HOSTS_LIST=${HH/ /}
}

function parse_hosts_file() {
  local HOSTS_FILE=$1
  local ORGHOSTSTR=
  if [ -f "${HOSTS_FILE}" ]; then
    ORGHOSTSTR=$(sed 's/#.*$//;/^$/d' "${HOSTS_FILE}" | xargs echo)
    if [ "$?" != "0" ]; then
      echo "ERROR: Get hosts from file ${HOSTS_FILE} error."
      exit 1
    fi

    parse_hosts_string ${ORGHOSTSTR}
  else
    echo "ERROR: File ${HOSTS_FILE} doesn't exist!"
    exit 1
  fi
}

OPERATION=

while [ $# != 0 ]; do
  case "$1" in
    "--hosts")
      shift
      hosts=
      while true
      do
        if [ x"$1" = x"" ]; then
          break
        fi
        host=$1
        ops=${host:0:1}
        if [ x"${ops}" = x"-" ]; then
          break
        fi
        hosts+=" $1"
        shift
      done
      parse_hosts_string ${hosts}
      ;;
    "--hostsfile")
      shift
      hostfile="$1"
      shift
      parse_hosts_file ${hostfile}
      ;;
    "--disable-smart-client")
      shift
      OPERATION=" touch /tmp/SMART_CLIENT_DISABLED_ID_FILE"
      ;;
    "--enable-smart-client")
      shift
      OPERATION=" rm -f /tmp/SMART_CLIENT_DISABLED_ID_FILE"
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done


if [ x"${HOSTS_LIST}" = x"" ]; then
  echo "No valid hosts specified."
else
  for host in ${HOSTS_LIST}
  do
    echo "Processing on ${host}"
    ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=3s ${host} ${OPERATION}
  done
fi