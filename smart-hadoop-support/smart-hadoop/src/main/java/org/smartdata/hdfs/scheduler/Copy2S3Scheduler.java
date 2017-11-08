package org.smartdata.hdfs.scheduler;

import org.smartdata.SmartContext;
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.ScheduleResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Copy2S3Scheduler extends ActionSchedulerService {
  private static final List<String> actions = Arrays.asList("copy2s3");
  private MetaStore metaStore;

  public Copy2S3Scheduler(SmartContext context, MetaStore metaStore) {
    super(context, metaStore);
    this.metaStore = metaStore;
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
