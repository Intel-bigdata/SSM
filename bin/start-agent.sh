# Start Smart Agents

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
      shift
      DEBUG_OPT="$1"
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
  --daemon start ${DEBUG_OPT} \
  smartagent -D smart.agent.master.address=${AGENT_MASTER}
