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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.client.SmartClient;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.model.FileState;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * DFSInputStream for SSM.
 */
public class SmartInputStream extends DFSInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(SmartInputStream.class);

  protected String src;
  protected final FileState fileState;
  protected SmartClient smartClient;
  private boolean healthy = false;

  SmartInputStream(DFSClient dfsClient, String src, boolean verifyChecksum,
      FileState fileState, SmartClient smartClient) throws IOException, UnresolvedLinkException {
    super(dfsClient, src, verifyChecksum);
    this.src = src;
    this.fileState = fileState;
    this.smartClient = smartClient;
    healthy = true;
  }

  @Override
  public synchronized int read(final byte buf[], int off, int len) throws IOException {
    if (super.getPos() == 0) {
      reportFileAccessEvent();
    }
    return super.read(buf, off, len);
  }

  @Override
  public synchronized int read(final ByteBuffer buf) throws IOException {
    if (super.getPos() == 0) {
      reportFileAccessEvent();
    }
    return super.read(buf);
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    if (position == 0) {
      reportFileAccessEvent();
    }
    return super.read(position, buffer, offset, length);
  }

  public FileState.FileType getType() {
    return fileState.getFileType();
  }

  /**
   * Report File Access Event whenever this InputStream reads from zero position.
   */
  protected void reportFileAccessEvent() {
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
}
