
package org.apache.hadoop.ssm.actions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.server.mover.Mover;
import org.apache.hadoop.util.ToolRunner;

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
    public boolean execute() {
        if(runMove(fileName)){
            return true;
        }else{
            return false;
        }
    }

    private boolean runMove(String fileName) {
        // TODO check if storagePolicy is the same
        if(storagePolicy.contains("COLD"))
            LOG.info("*" + System.currentTimeMillis() + " : " + fileName + " -> " + "archive");
        else
            LOG.info("*" + System.currentTimeMillis() + " : " + fileName + " -> " + "ssd");
        try {
            dfsClient.setStoragePolicy(fileName, storagePolicy);
        } catch (Exception e) {
            return false;
        }
        try {
            ToolRunner.run(conf, new Mover.Cli(),
                    new String[]{"-p", fileName});
        } catch (Exception e) {
            return false;
        }
        return true;
    }


}