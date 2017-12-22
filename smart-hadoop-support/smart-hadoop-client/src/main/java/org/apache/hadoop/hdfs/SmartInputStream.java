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
import org.apache.hadoop.util.VersionInfo;
import org.apache.htrace.TraceScope;
import org.smartdata.model.FileState;

import java.io.IOException;

/**
 * DFSInputStream for SSM.
 */
public abstract class SmartInputStream extends DFSInputStream {
  protected final FileState fileState;

  private static final String HADOOP_26_VERSION = "2.6";
  private static final String HADOOP_27_VERSION = "2.7";

  SmartInputStream(DFSClient dfsClient, String src, boolean verifyChecksum,
      FileState fileState) throws IOException, UnresolvedLinkException {
    super(dfsClient, src, verifyChecksum);
    this.fileState = fileState;
  }

  public FileState.FileType getType() {
    return fileState.getFileType();
  }

  private static String getHadoopVersion() {
    String version = VersionInfo.getVersion();
    String[] parts = version.split("\\.");
    if (parts.length < 2) {
      throw new RuntimeException("Illegal Hadoop Version: " + version + " (expected A.B.* format)");
    }
    Integer first = Integer.parseInt(parts[0]);
    if (first == 0 || first == 1) {
      throw new RuntimeException("Hadoop version 0.x and 1.x are not supported");
    }
    Integer second = Integer.parseInt(parts[1]);
    if (first == 2 && second <= 6) {
      return HADOOP_26_VERSION;
    } else {
      return HADOOP_27_VERSION;
    }
  }

  public static DFSInputStream create(DFSClient dfsClient, String src,
      boolean verifyChecksum, FileState fileState)
      throws IOException, UnresolvedLinkException {
    dfsClient.checkOpen();
    if (getHadoopVersion().equals(HADOOP_26_VERSION)) {
      DFSInputStream inputStream = null;
      switch (fileState.getFileType()) {
        case NORMAL:
          inputStream = new DFSInputStream(dfsClient, src, verifyChecksum);
          break;
        case COMPACT:
          inputStream = new CompactInputStream(dfsClient, src, verifyChecksum, fileState);
          break;
        case COMPRESSION:
          inputStream = new CompressionInputStream(dfsClient, src, verifyChecksum, fileState);
          break;
        case S3:
          inputStream = new S3InputStream(dfsClient, src, verifyChecksum, fileState);
          break;
        default:
          throw new IOException("Unsupported file type");
      }
      return inputStream;
    } else {
      TraceScope scope = dfsClient.getPathTraceScope("newDFSInputStream", src);
      try {
        DFSInputStream inputStream = null;
        switch (fileState.getFileType()) {
          case NORMAL:
            inputStream = new DFSInputStream(dfsClient, src, verifyChecksum);
            break;
          case COMPACT:
            inputStream = new CompactInputStream(dfsClient, src, verifyChecksum, fileState);
            break;
          case COMPRESSION:
            inputStream = new CompressionInputStream(dfsClient, src, verifyChecksum, fileState);
            break;
          case S3:
            inputStream = new S3InputStream(dfsClient, src, verifyChecksum, fileState);
            break;
          default:
            throw new IOException("Unsupported file type");
        }
        return inputStream;
      } finally {
        scope.close();
      }
    }
  }
}
