package org.smartdata.action;

import org.smartdata.action.annotation.ActionSignature;

/**
 * Sync action is an abstract action for backup and copy
 * Users can submit a sync action with detailed src (path) and optional
 * dest(path), e.g., "sync -src /test/1 -dest hdfs:/remoteIP:port/test/1",
 * "sync -src /test/1, if backup rule scanning /test is configured.
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
  // optional, related to remote cluster and fileDiff.src
  public static final String DEST = "-dest";

  @Override
  protected void execute() throws Exception {
    this.appendResult(String
        .format("Sync %s", getArguments().get(SRC)));
    if (getArguments().containsKey(DEST)) {
      this.appendResult(String
          .format(" to %s\n",
              getArguments().get(DEST)));
    } else {
      this.appendResult("\n");
    }
  }
}
