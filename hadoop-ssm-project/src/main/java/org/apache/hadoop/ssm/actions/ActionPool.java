package org.apache.hadoop.ssm.actions;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Action Pool
 */
public class ActionPool {

    private static ActionPool instance = new ActionPool();

    private ActionPool() {
    }

    public static ActionPool getInstance() {
        return  instance;
    }

    public void getActionStatus() {

    }

}
