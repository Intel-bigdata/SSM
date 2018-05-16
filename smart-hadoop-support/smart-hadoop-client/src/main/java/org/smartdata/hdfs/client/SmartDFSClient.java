/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.hdfs.client;

import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.SmartInputStreamFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartConstants;
import org.smartdata.client.SmartClient;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.model.FileState;
import org.smartdata.model.S3FileState;
import org.smartdata.model.FileState.FileStage;
import org.smartdata.model.FileState.FileType;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

public class SmartDFSClient extends DFSClient {
  private static final Logger LOG = LoggerFactory.getLogger(SmartDFSClient.class);
  private SmartClient smartClient = null;
  private boolean healthy = false;
  private Configuration conf;

  public SmartDFSClient(InetSocketAddress nameNodeAddress, Configuration conf,
      InetSocketAddress smartServerAddress) throws IOException {
    super(nameNodeAddress, conf);
    if (isSmartClientDisabled()) {
      return;
    }
    try {
      smartClient = new SmartClient(conf, smartServerAddress);
      healthy = true;
    } catch (IOException e) {
      super.close();
      throw e;
    }
  }

  public SmartDFSClient(final URI nameNodeUri, final Configuration conf,
      final InetSocketAddress smartServerAddress) throws IOException {
    super(nameNodeUri, conf);
    if (isSmartClientDisabled()) {
      return;
    }
    try {
      smartClient = new SmartClient(conf, smartServerAddress);
      healthy = true;
    } catch (IOException e) {
      super.close();
      throw e;
    }
  }

  public SmartDFSClient(URI nameNodeUri, Configuration conf,
      FileSystem.Statistics stats, InetSocketAddress smartServerAddress)
      throws IOException {
    super(nameNodeUri, conf, stats);
    if (isSmartClientDisabled()) {
      return;
    }
    try {
      smartClient = new SmartClient(conf, smartServerAddress);
      healthy = true;
    } catch (IOException e) {
      super.close();
      throw e;
    }
  }

  public SmartDFSClient(Configuration conf,
      InetSocketAddress smartServerAddress) throws IOException {
    super(conf);
    if (isSmartClientDisabled()) {
      return;
    }
    try {
      smartClient = new SmartClient(conf, smartServerAddress);
      healthy = true;
    } catch (IOException e) {
      super.close();
      throw e;
    }
  }

  public SmartDFSClient(Configuration conf) throws IOException {
    super(conf);
    if (isSmartClientDisabled()) {
      return;
    }
    try {
      smartClient = new SmartClient(conf);
      healthy = true;
    } catch (IOException e) {
      super.close();
      throw e;
    }
  }

  @Override
  public DFSInputStream open(String src)
      throws IOException, UnresolvedLinkException {
    return super.open(src);
  }

  @Override
  public DFSInputStream open(String src, int buffersize,
      boolean verifyChecksum)
      throws IOException, UnresolvedLinkException {
    FileState fileState;
    src = checkS3ForXattr(src);
    if (healthy) { // TODO: required indeed
      fileState = smartClient.getFileState(src);
      if (fileState.getFileStage().equals(FileState.FileStage.PROCESSING)) {
        throw new IOException("Cannot open " + src + " when it is under PROCESSING to "
            + fileState.getFileType());
      }
    } 
    else if (src.startsWith("s3a")) {
      fileState = new S3FileState(src);
    }
    else {
      fileState = new FileState(src, FileType.NORMAL, FileStage.DONE);
    }
    DFSInputStream is = SmartInputStreamFactory.get().create(this, src,
        verifyChecksum, fileState);
    reportFileAccessEvent(src);
    return is;
  }

  @Deprecated
  @Override
  public DFSInputStream open(String src, int buffersize,
      boolean verifyChecksum, FileSystem.Statistics stats)
      throws IOException, UnresolvedLinkException {
    return super.open(src, buffersize, verifyChecksum, stats);
  }

  private void reportFileAccessEvent(String src) {
    try {
      if (!healthy) {
        return;
      }
      String userName;
      try {
        userName = UserGroupInformation.getCurrentUser().getUserName();
      } catch (IOException e) {
        userName = "Unknown";
      }
      smartClient.reportFileAccessEvent(new FileAccessEvent(src, userName));
    } catch (IOException e) {
      // Here just ignores that failed to report
      LOG.error("Cannot report file access event to SmartServer: " + src
          + " , for: " + e.getMessage()
          + " , report mechanism will be disabled now in this instance.");
      healthy = false;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    try {
      super.close();
    } catch (IOException e) {
      throw e;
    } finally {
      try {
        if (smartClient != null) {
          smartClient.close();
        }
      } finally {
        healthy = false;
      }
    }
  }

  private boolean isSmartClientDisabled() {
    File idFile = new File(SmartConstants.SMART_CLIENT_DISABLED_ID_FILE);
    return idFile.exists();
  }

  public String checkS3ForXattr(String filePath) throws IOException {
    // I think we should return the true path if file is move to S3,
    // or return filePath If Xattr doesn't contain the attribute we want
    // For example, if file1 is move to s3a://XX/file1, then we can return
    // s3a://XX/file1. If not, we just return filePath
       

     LocatedBlocks  locatedBlocks  = super.getLocatedBlocks(filePath.toString(),  0);
     if(locatedBlocks!=null  &&  locatedBlocks.getFileLength()  ==  0){
        byte[]  xAttr  =  super.getXAttr(filePath,  "user.coldloc");
        if(xAttr!=null){
             //Input  stream  to  read  the  data  directly  from  s3fs
           filePath = new  String(xAttr) ;  
        }
     }
     return filePath;
  }
}
