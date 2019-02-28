package org.smartdata.server.engine.cmdlet.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

import static org.smartdata.SmartConstants.NUMBER_OF_SMART_AGENT;

public class AgentHosts {

    private final String agentConfFile = "agents";
    private SmartConf conf;
    public AgentHosts(SmartConf conf) {
        this.conf = conf;
    }

    public Set<String> getHosts() {
        String agentConfFile = conf.get(SmartConfKeys.SMART_CONF_DIR_KEY,
                SmartConfKeys.SMART_CONF_DIR_DEFAULT) + "/agents";
        Scanner sc = null;
        HashSet<String> hosts = new HashSet<>();
        try {
            sc = new Scanner(new File(agentConfFile));
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }

        while (sc != null && sc.hasNextLine()) {
            String host = sc.nextLine().trim();
            if (!host.startsWith("#") && !host.isEmpty()) {
                hosts.add(host);
            }
        }

        return hosts;
    }
}
