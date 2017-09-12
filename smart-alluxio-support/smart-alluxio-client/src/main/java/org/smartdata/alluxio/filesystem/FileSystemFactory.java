package org.smartdata.alluxio.filesystem;

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;

import java.io.IOException;

/**
 * Factory for {@link FileSystemFactory}.
 * Usage:
 * FileSystem fs = FileSystemFactory.get();
 * InputStream is = fs.openFile(new AlluxioURI("/path1/file1"));
 */
public class FileSystemFactory {

  private FileSystemFactory() {} // prevent instantiation

  public static FileSystem get() throws IOException {
    return SmartAlluxioBaseFileSystem.get(FileSystemContext.INSTANCE);
  }

  public static FileSystem get(FileSystemContext context) {
    return SmartAlluxioBaseFileSystem.get(context);
  }

}
