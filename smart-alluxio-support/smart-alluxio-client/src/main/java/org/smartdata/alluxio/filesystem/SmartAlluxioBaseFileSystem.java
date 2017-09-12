package org.smartdata.alluxio.filesystem;

import alluxio.AlluxioURI;
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.client.SmartClient;
import org.smartdata.conf.SmartConf;
import org.smartdata.metrics.FileAccessEvent;

import java.io.IOException;

public class SmartAlluxioBaseFileSystem extends BaseFileSystem {
  protected static final Logger LOG = LoggerFactory.getLogger(SmartAlluxioBaseFileSystem.class);

  private SmartClient smartClient = null;

  protected SmartAlluxioBaseFileSystem(FileSystemContext context) {
    super(context);
    try {
      smartClient = new SmartClient(new SmartConf());
    } catch (IOException e) {
      LOG.error(e.getMessage());
      System.exit(-1);
    }
  }

  /**
   * @param context file system context
   * @return a {@link SmartAlluxioBaseFileSystem}
   */
  public static SmartAlluxioBaseFileSystem get(FileSystemContext context) {
    return new SmartAlluxioBaseFileSystem(context);
  }

  @Override
  public FileInStream openFile(AlluxioURI uri)
      throws FileDoesNotExistException, IOException, AlluxioException {
    reportFileAccessEvent(uri.getPath());
    return super.openFile(uri);
  }

  @Override
  public FileInStream openFile(AlluxioURI uri, OpenFileOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    FileInStream fis = super.openFile(uri, options);
    reportFileAccessEvent(uri.getPath());
    return fis;
  }

  private void reportFileAccessEvent(String src) {
    try {
      smartClient.reportFileAccessEvent(new FileAccessEvent(src));
    } catch (IOException e) {
      LOG.error("Can not report file access event to SmartServer: " + src);
    }
  }
}
