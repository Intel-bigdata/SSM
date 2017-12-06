# Start Smart Agents

bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "${bin}">/dev/null; pwd)

. "${bin}/common.sh"

#Running start-agent.sh with no host option will start an agent on localhost
AGENT_HOSTS=localhost

while [ $# != 0 ]; do
  case "$1" in
    "--config")
      shift
      SMART_CONF_DIR="$1"
      shift
      ;;
    "--host")
      shift
      AGENT_HOSTS="$1"
      shift
      ;;
    "--debug")
      shift
      DEBUG_OPT="$1"
      shift
      ;;
    *)
      break;
      ;;
  esac
done

AGENT_MASTER=${SMARTSERVERS// /,}

AH=
for i in $AGENT_HOSTS; do if [ "$i" = "localhost" ]; then AH+=" ${HOSTNAME}" ; else AH+=" $i"; fi; done
  AGENT_HOSTS=${AH/ /}

echo "Starting SmartAgents on [${AGENT_HOSTS}]"
sleep 3
. "${SMART_HOME}/bin/ssm" \
  --remote \
  --config "${SMART_CONF_DIR}" \
  --hosts "${AGENT_HOSTS}" --hostsend \
  --daemon start ${DEBUG_OPT} \
  smartagent -D smart.agent.master.address=${AGENT_MASTER}
