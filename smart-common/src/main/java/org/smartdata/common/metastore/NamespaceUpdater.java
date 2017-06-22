package org.smartdata.common.metastore;

public interface NamespaceUpdater {

  void insertFile();

  void insertFiles();

  void updateFile(FileInfoMapper fileInfoMapper);

  void deleteFile(long fid);

  void deleteDirectory(String dirPath);

  void deleteAllFiles();
}
