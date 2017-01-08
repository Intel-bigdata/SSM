package org.apache.hadoop.ssm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.FilesAccessInfo;
import org.apache.hadoop.ssm.api.Expression.*;
import org.apache.hadoop.ssm.parse.SSMRuleParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by root on 11/1/16.
 */
public class SSMServer {
  public static final Configuration conf;
  static {
    conf = new HdfsConfiguration();
    Path path = new Path("/home/sorttest/hadoop/etc/hadoop/core-site.xml");
    //Path path = new Path("/home/hadoop_src/hadoop-2.7.2/hadoop-dist/target/hadoop-2.7.2/etc/hadoop/core-site.xml");
    conf.addResource(path);
  }
  public static final Logger LOG = LoggerFactory.getLogger(SSMServer.class);

  static class DecisionMakerTask extends TimerTask {
    private DFSClient dfsClient;
    private DecisionMaker decisionMaker;

    public DecisionMakerTask(DFSClient dfsClient, DecisionMaker decisionMaker) {
      this.dfsClient = dfsClient;
      this.decisionMaker = decisionMaker;
    }

    @Override
    public void run() {
      //LOG.info("Update all information:");
      System.out.println("========Update all information========");
      FilesAccessInfo filesAccessInfo;
      try {
        filesAccessInfo = dfsClient.getFilesAccessInfo();
        //LOG.info("Number of accessed files = " + filesAccessInfo.getFilesAccessed().size());
        System.out.println("1. filesAccessInfo");
        for (int i = 0; i < filesAccessInfo.getFilesAccessed().size(); i++) {
          System.out.println(filesAccessInfo.getFilesAccessed().get(i) + "\t" + filesAccessInfo.getFilesAccessCounts().get(i));
        }
      } catch (Exception e) {
        //LOG.warn("getFilesAccessInfo exception");
        throw new RuntimeException(e);
      }
      decisionMaker.execution(dfsClient, conf, filesAccessInfo);
    }
  }

  public static void main(String[] args) throws Exception {
    DFSClient dfsClient = new DFSClient(DFSUtilClient.getNNAddress(conf), conf);
    long updateDuration = 1*60;

    DecisionMaker decisionMaker = new DecisionMaker(dfsClient, conf, updateDuration);
    SSMRule ruleObject = SSMRuleParser.parseAll("file.path matches('/home/SSDtest/[a-z]*') : accessCount (3 min) >= 10 | ssd").get();
    //decisionMaker.addRule(ruleObject);
    ruleObject = SSMRuleParser.parseAll("file.path matches('/home/ARCHIVEtest/[a-z]*') : age >= 1200000 | archive").get(); //20min
    //decisionMaker.addRule(ruleObject);
    ruleObject = SSMRuleParser.parseAll("file.path matches('/home/CACHEtest/[a-z]*') : accessCount (3 min) >= 10 | cache").get(); //20min
    decisionMaker.addRule(ruleObject);

    //LOG.info("Initialization completed");
    System.out.println("Initialization completed");

    MoverExecutor.getInstance(dfsClient, conf).run();

    Timer timer = new Timer();
    timer.schedule(new DecisionMakerTask(dfsClient, decisionMaker), 2*1000L, updateDuration*1000L);
  }
}
