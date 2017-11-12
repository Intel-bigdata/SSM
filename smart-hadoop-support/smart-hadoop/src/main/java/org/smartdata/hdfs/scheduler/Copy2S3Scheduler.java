package org.smartdata.hdfs.scheduler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.ScheduleResult;

import java.io.IOException;
import java.net.URI;
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
      conf = getContext().getConf();
    } catch (NullPointerException e) {
      // If SmartContext is empty
      conf = new Configuration();
    }
  }

  private void lockTheFile(String filePath) {
    fileLock.add(filePath);
  }

  private void unLockTheFile(String filePath) {
    fileLock.remove(filePath);
  }

  private long checkTheLengthOfFile(String fileName) {
    FileSystem fs = null;
    try {
      fs = FileSystem.get(URI.create(fileName), conf);
      return fs.getFileStatus(new Path(fileName)).getLen();
    } catch (IOException e) {
      LOG.error("Fetch file length error!", e);
    }
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
    return true;
  }

  @Override
  public void onActionFinished(ActionInfo actionInfo) {
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
