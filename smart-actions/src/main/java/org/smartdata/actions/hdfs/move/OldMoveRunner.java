package org.smartdata.actions.hdfs.move;

/**
 * Created by root on 17-7-31.
 */
public abstract class OldMoveRunner {

    public abstract void move(String file) throws Exception;

    public abstract void move(String[] files) throws Exception;
}
