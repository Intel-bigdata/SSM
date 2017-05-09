package org.apache.hadoop.ssm.actions;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by QYGong on 05/05/2017.
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
