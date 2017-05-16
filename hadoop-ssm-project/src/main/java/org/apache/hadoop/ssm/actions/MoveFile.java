
package org.apache.hadoop.ssm.actions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.server.mover.Mover;
import org.apache.hadoop.ssm.mover.MoverPool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Date;
import java.util.UUID;

/**
 * MoveFile Action
 */
public class MoveFile extends ActionBase {
    private static final Log LOG = LogFactory.getLog(MoveFile.class);
    private static MoveFile instance;
    public String storagePolicy;
    private String fileName;
    private Configuration conf;

    public MoveFile(DFSClient client, Configuration conf, String storagePolicy) {
        super(client);
        this.conf = conf;
        this.actionType = ActionType.MoveFile;
        this.storagePolicy = storagePolicy;
    }

    public static synchronized MoveFile getInstance(DFSClient dfsClient, Configuration conf, String storagePolicy) {
        if (instance == null) {
            instance = new MoveFile(dfsClient, conf, storagePolicy);
        }
        return instance;
    }

    public void initial(String[] args) {
        this.fileName = args[0];
    }

    /**
     * Execute an action.
     *
     * @return true if success, otherwise return false.
     */
    public UUID execute() {
        return runMove(fileName);
    }

    private UUID runMove(String fileName) {
        // TODO check if storagePolicy is the same
        LOG.info("Action starts at " + new Date(System.currentTimeMillis())
            + " : " + fileName + " -> " + storagePolicy);
        try {
            dfsClient.setStoragePolicy(fileName, storagePolicy);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        MoverPool.getInstance().createMoverAction(fileName);
        return null;
    }


}