package org.smartdata.model;

/**
 * FileState for normal S3 files.
 */
public class S3FileState extends FileState {

  public S3FileState(String path) {
    super(path, FileType.S3, FileStage.DONE);
  }
}
