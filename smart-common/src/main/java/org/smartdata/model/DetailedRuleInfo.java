package org.smartdata.model;

public class DetailedRuleInfo extends RuleInfo {
  public long baseProgress;
  public long runningProgress;

  public DetailedRuleInfo(RuleInfo ruleInfo) {
    // Init from ruleInfo
    super(ruleInfo.getId(), ruleInfo.getSubmitTime(), ruleInfo.getRuleText(), ruleInfo.getState(),
        ruleInfo.getNumChecked(), ruleInfo.getNumCmdsGen(), ruleInfo.getLastCheckTime());
  }

  public long getBaseProgress() {
    return baseProgress;
  }

  public void setBaseProgress(long baseProgress) {
    this.baseProgress = baseProgress;
  }

  public long getRunningProgress() {
    return runningProgress;
  }

  public void setRunningProgress(long runningProgress) {
    this.runningProgress = runningProgress;
  }


}
