package org.smartdata.hdfs.action;

import java.util.Map;

public class CheckErasureCodingPolicy extends HdfsAction {
  private String srcPath;

  @Override
  public void init(Map<String, String> args) {
    this.srcPath = args.get(HdfsAction.FILE_PATH);
  }

  @Override
  public void execute() throws Exception {
    appendResult(dfsClient.getErasureCodingPolicy(srcPath).toString());
  }
}
