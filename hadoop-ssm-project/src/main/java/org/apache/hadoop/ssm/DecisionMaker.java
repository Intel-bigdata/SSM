package org.apache.hadoop.ssm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.FilesAccessInfo;
import org.apache.hadoop.ssm.api.Expression.SSMRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by root on 10/31/16.
 */
public class DecisionMaker {
  public static final Logger LOG = LoggerFactory.getLogger(DecisionMaker.class);

  private FileAccessMap fileMap;
  private HashMap<Long, RuleContainer> ruleMaps;

  private DFSClient dfsClient;
  private Configuration conf;
  private Long updateDuration; // second
  private Boolean isInitialized;

  private HashMap<Long, SSMRule> newRules;

  public DecisionMaker(DFSClient dfsClient, Configuration conf, Long updateDurationInSecond) {
    fileMap = new FileAccessMap();
    ruleMaps = new HashMap<Long, RuleContainer>();
    newRules = new HashMap<Long, SSMRule>();
    this.dfsClient = dfsClient;
    this.conf = conf;
    this.updateDuration = updateDurationInSecond;
    isInitialized = false;
  }

  /**
   * Add new rules to this class
   * @param ssmRule
   */
  synchronized public void addRule(SSMRule ssmRule) {
    newRules.put(ssmRule.getId(), ssmRule);
  }

  /**
   * Update rules each time doing execution
   */
  synchronized private Integer updateRules() {
    for (Map.Entry<Long, SSMRule> entry : newRules.entrySet()) {
      Long ruleId = entry.getKey();
      SSMRule ssmRule = entry.getValue();
      RuleContainer ruleContainer = new RuleContainer(ssmRule, updateDuration, dfsClient);
      ruleMaps.put(ruleId, ruleContainer);
    }
    try {
      return newRules.size();
    } finally {
      newRules.clear();
    }
  }

  /**
   * Read FilesAccessInfo to refresh file information of DecisionMaker
   * @param filesAccessInfo
   */
  public void getFilesAccess(FilesAccessInfo filesAccessInfo){
    // update fileMap
    fileMap.updateFileMap(filesAccessInfo);
    // process nnEvent
    fileMap.processNnEvents(filesAccessInfo);

    // update ruleMaps
    updateRules();
    if (!isInitialized) {
      isInitialized = true;
    }
    else {
      for (Map.Entry<Long, RuleContainer> entry : ruleMaps.entrySet()) {
        entry.getValue().update(filesAccessInfo);
      }
    }
  }

  /**
   * Run Mover tool to move a file.
   * @param fileNames
   * @param actions
   */
  private void runExecutor(List<String> fileNames, List<Action> actions) {
    for (int i = 0; i < fileNames.size(); i++) {
      String fileName = fileNames.get(i);
      Action action = actions.get(i);
      MoverExecutor.getInstance().addActionEvent(fileName, action);
    }
  }


  public void execution(DFSClient dfsClient, Configuration conf, FilesAccessInfo filesAccessInfo) {
    // update information
    getFilesAccess(filesAccessInfo);

    // debug
    System.out.println("2. fileMap");
    for (Map.Entry<String, FileAccess> entry : fileMap.entrySet()) {
      System.out.println(entry.getKey() + "\t" + entry.getValue().getAccessCount());
    }


    // run executor
    List<String> fileNames = new LinkedList<String>();
    List<Action> actions = new LinkedList<Action>();
    for (Map.Entry<Long, RuleContainer> ruleMapsEntry : ruleMaps.entrySet()) {
      HashMap<String, Action> fileAction = ruleMapsEntry.getValue().actionEvaluator(fileMap);
      for (Map.Entry<String, Action> fileActionEntry : fileAction.entrySet()) {
        fileNames.add(fileActionEntry.getKey());
        actions.add(fileActionEntry.getValue());
      }
    }
    runExecutor(fileNames, actions);
  }

  public HashMap<Long, RuleContainer> getRuleMaps() {
    return ruleMaps;
  }

  public FileAccessMap getFileMap() {
    return fileMap;
  }
}
