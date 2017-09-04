package org.smartdata.action;

import org.smartdata.action.annotation.ActionSignature;

/**
 * Sync action is an abstract action for backup and copy
 */
@ActionSignature(
    actionId = "sync",
    displayName = "sync",
    usage = SyncAction.FILE + " $file" + SyncAction.DIFF + " $diffId"
)
public class SyncAction extends SmartAction {

  // related to fileDiff.src
  public static final String FILE = "-file";
  // related to fileDiff.diffId
  public static final String DIFF = "-diff";

  @Override
  protected void execute() throws Exception {
    this.appendResult(String
        .format("Sync %s based on file diff %s\n", getArguments().get(FILE),
            getArguments().get(DIFF)));
  }
}
