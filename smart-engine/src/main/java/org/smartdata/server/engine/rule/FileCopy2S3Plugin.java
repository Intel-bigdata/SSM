package org.smartdata.server.engine.rule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.rule.RuleExecutorPlugin;
import org.smartdata.model.rule.TranslateResult;

import java.util.List;

public class FileCopy2S3Plugin implements RuleExecutorPlugin {

  private static final Logger LOG =
      LoggerFactory.getLogger(FileCopy2S3Plugin.class.getName());

  @Override
  public void onNewRuleExecutor(RuleInfo ruleInfo,
      TranslateResult tResult) {
  }

  @Override
  public boolean preExecution(RuleInfo ruleInfo,
      TranslateResult tResult) {
    return true;
  }

  @Override
  public List<String> preSubmitCmdlet(RuleInfo ruleInfo,
      List<String> objects) {
    return objects;
  }

  @Override
  public CmdletDescriptor preSubmitCmdletDescriptor(RuleInfo ruleInfo,
      TranslateResult tResult, CmdletDescriptor descriptor) {
    return descriptor;
  }

  @Override
  public void onRuleExecutorExit(RuleInfo ruleInfo) {

  }
}
