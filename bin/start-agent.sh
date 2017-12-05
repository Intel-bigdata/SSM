# Start Smart Agents

bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "${bin}">/dev/null; pwd)

. "${bin}/common.sh"

SMART_CONF_DIR=$SMART_HOME/conf
AGENT_HOST=localhost
while [ $# != 0 ]; do
  case "$1" in
    "--config")
      shift
      SMART_CONF_DIR="$1"
      shift
      ;;
      "--host")
        shift
        AGENT_HOST="$1"
        shift
        ;;
    *)
      break;
      ;;
  esac
done

AGENT_MASTER=${SMARTSERVERS// /,}

if [ "$AGENT_HOST" = "localhost" ]; then
  AGENT_HOST=" ${HOSTNAME}" ;
fi
echo "Starting SmartAgents on [${AGENT_HOST}]"
sleep 3
. "${SMART_HOME}/bin/ssm" \
  --remote \
  --config "${SMART_CONF_DIR}" \
  --hosts "${AGENT_HOST}" --hostsend \
  --daemon start ${DEBUG_OPT} \
  smartagent -D smart.agent.master.address=${AGENT_MASTER}
