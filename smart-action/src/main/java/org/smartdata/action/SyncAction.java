package org.smartdata.action;

import org.smartdata.action.annotation.ActionSignature;

/**
 * Sync action is an abstract action for backup and copy
 * Users can submit a sync action with detailed src (path) and dest(path),
 * e.g., sync -src /test/1 -dest hdfs:/remoteIP:port/test/1
 */
@ActionSignature(
    actionId = "sync",
    displayName = "sync",
    usage = SyncAction.SRC + " $src" + SyncAction.DEST + " $dest"
)
public class SyncAction extends SmartAction {

  // TODO add dir support
  // related to fileDiff.src
  public static final String SRC = "-src";
  // related to remote cluster + fileDiff.src
  public static final String DEST = "-dest";
  // optional, related to fileDiff.diffId
  public static final String DIFF = "-diff";

  @Override
  protected void execute() throws Exception {
    this.appendResult(String
        .format("Sync from %s to %s\n", getArguments().get(SRC),
            getArguments().get(DEST)));
    if (getArguments().containsKey(DIFF)) {
      this.appendResult(String
          .format("Sync based on file diff %s\n",
              getArguments().get(DIFF)));
    }
  }
}
