package org.apache.hadoop.ssm.actions;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Action Pool
 */
public class ActionPool {

    private static ActionPool instance = new ActionPool();
    private Map<UUID, ActionBase> actionBaseMap;

    private ActionPool() {
        actionBaseMap = new ConcurrentHashMap<>();
    }

    pubice static ActionPool getInstance() {
        return  instance;
    }

    public UUID runCommand(String ) {

    }

    public getActionStatus() {

    }

}
