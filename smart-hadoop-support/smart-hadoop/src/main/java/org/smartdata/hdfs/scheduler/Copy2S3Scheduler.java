package org.smartdata.hdfs.scheduler;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.hdfs.action.HdfsAction;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.ScheduleResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class Copy2S3Scheduler extends ActionSchedulerService {
  private static final List<String> actions = Arrays.asList("copy2s3");
  static final Logger LOG =
      LoggerFactory.getLogger(Copy2S3Scheduler.class);
  private MetaStore metaStore;
  //The file in copy need to be locked
  private Set<String> fileLock;
  // Global variables
  private Configuration conf;

  public Copy2S3Scheduler(SmartContext context, MetaStore metaStore) {
    super(context, metaStore);
    this.metaStore = metaStore;
    this.fileLock = Collections.synchronizedSet(new HashSet<String>());
    try {
      this.conf = getContext().getConf();
    } catch (NullPointerException e) {
      // If SmartContext is empty
      this.conf = new Configuration();
    }
  }

  private void lockTheFile(String filePath) {
    fileLock.add(filePath);
  }

  private void unLockTheFile(String filePath) {
    fileLock.remove(filePath);
  }

  private boolean ifLocked(String filePath) {
    return fileLock.contains(filePath);
  }

  private long checkTheLengthOfFile(String fileName) {
    try {
      return metaStore.getFile(fileName).getLength();
    } catch (MetaStoreException e) {
      e.printStackTrace();
    }
    return 0;
  }

  public List<String> getSupportedActions() {
    return actions;
  }

  @Override
  public ScheduleResult onSchedule(ActionInfo actionInfo, LaunchAction action) {
    return ScheduleResult.SUCCESS;
  }

  @Override
  public boolean onSubmit(ActionInfo actionInfo) {
    String path = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
    if (checkTheLengthOfFile(path) == 0) {
      LOG.info("The submit file {}'s length is 0", path);
      return false;
    }

    if (ifLocked(path)) {
      LOG.info("The submit file {} is locked", path);
      return false;
    }

    lockTheFile(path);

    LOG.info("The file {} can be submited", path);
    return true;
  }

  @Override
  public void onActionFinished(ActionInfo actionInfo) {
    String path = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
    // unlock filelock
    if (ifLocked(path)) {
      unLockTheFile(path);
    } else {
      LOG.info("The file {} has already unlocked", path);
    }
  }

  @Override
  public void init() throws IOException {
  }

  @Override
  public void start() throws IOException {

  }

  @Override
  public void stop() throws IOException {
  }

}
