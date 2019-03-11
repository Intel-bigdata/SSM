package org.smartdata;

import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

public class NodeHosts {

    private SmartConf conf;
    public Set<String> agentHosts;
    public Set<String> serverHosts;
    public NodeHosts(SmartConf conf) {
        this.conf = conf;
        this.serverHosts = init("server");
        this.agentHosts = init("agent");
    }

    public Set<String> init(String role) {
        String fileName = "/agents";
        switch (role) {
            case "agent":
                fileName = "/agents";
                break;
            case "server":
                fileName = "/servers";
                break;
        }
        String hostName = "";
        try {
            InetAddress address = InetAddress.getLocalHost();
            hostName = address.getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        String agentConfFile = conf.get(SmartConfKeys.SMART_CONF_DIR_KEY,
                SmartConfKeys.SMART_CONF_DIR_DEFAULT) + fileName;
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
                if (host.equals("localhost")) {
                    hosts.add(hostName);
                } else {
                    hosts.add(host);
                }
            }
        }

        return hosts;
    }

    public Set<String> getServerHosts() {
        return serverHosts;
    }

    public Set<String> getAgentHosts() {
        return agentHosts;
    }
}
