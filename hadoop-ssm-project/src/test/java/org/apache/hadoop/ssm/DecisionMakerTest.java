package org.apache.hadoop.ssm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.FilesAccessInfo;
import org.apache.hadoop.ssm.api.Expression.*;
import org.apache.hadoop.ssm.parse.SSMRuleParser;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by root on 11/1/16.
 */
public class DecisionMakerTest {
  public static final Configuration conf;
  static {
    conf = new HdfsConfiguration();
  }

  @Test
  public void testFileActions() {
    SSMRule ruleObject = SSMRuleParser.parseAll("file.path matches('/A/[a-z]*') : accessCount (10 min) >= 50 | ssd").get();
    long updateDuration = 1*60;
    DFSClient dfsClient = null;
    try {
      dfsClient = new DFSClient(DFSUtilClient.getNNAddress(conf), conf);
    } catch (Exception e) {
    }

    DecisionMaker decisionMaker = new DecisionMaker(dfsClient, conf, updateDuration);
    decisionMaker.addRule(ruleObject);
    //int[] accessCount = {0,1,1,4,4,2,2,10,10,1,1,5,5,10,10,7,7,1,1,2,2};
    int[] accessCount = {23,1,1,4,4,2,2,10,10,1,1,5,5,10,10,7,7,1,1,2,2};
    FilesAccessInfo filesAccessInfo = new FilesAccessInfo();
    for (int i = 0; i < accessCount.length; i++) {
      HashMap<String, Integer> map = new HashMap<String, Integer>();
      map.put("/A/a",accessCount[i]);
      filesAccessInfo.setAccessCounter(map);

      // update decisionMaker
      decisionMaker.getFilesAccess(filesAccessInfo);

      FileAccessMap fileMap = decisionMaker.getFileMap();
      HashMap<Long, RuleContainer> ruleMaps = decisionMaker.getRuleMaps();
      for (Map.Entry<Long, RuleContainer> entry : ruleMaps.entrySet()) {
        RuleContainer ruleContainer = entry.getValue();
        HashMap<String, Action> fileActions = ruleContainer.actionEvaluator(fileMap);
        switch (i) {
          case 13:
          case 14:
          case 15:
          case 16:
          case 17:
          case 20:
            assertEquals(1, fileActions.size());
            assertEquals(Action.SSD, fileActions.get("/A/a"));
            break;
          default:
            assertEquals(0, fileActions.size());
        }
      }
    }
  }
}
