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
package org.smartdata.hdfs.action;

import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.ActionException;
import org.smartdata.action.Utils;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.CompatibilityHelperLoader;

import java.util.Random;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.EnumSet;
import java.security.MessageDigest;
import java.util.regex.*;  

/**
 * An action to copy a single file from src to destination.
 * If dest doesn't contains "hdfs" prefix, then destination will be set to
 * current cluster, i.e., copy between dirs in current cluster.
 * Note that destination should contains filename.
 * If -useSingleBucketPerSession option is passed into copy2s3 action,
 * All the files in a particular SessionId will be placed in single bucket
 * for easy access
 */
@ActionSignature(
    actionId = "copy2s3",
    displayName = "copy2s3",
    usage = HdfsAction.FILE_PATH + " $src " + Copy2S3Action.DEST +
        " $dest "
)
public class Copy2S3Action extends HdfsAction {
  private static final Logger LOG =
      LoggerFactory.getLogger(CopyFileAction.class);
  public static final String BUF_SIZE = "-bufSize";
  public static final String USE_SINGLE_BUCKET_PER_SESSION = "-useSingleBucketPerSession";
  public static final String SRC = HdfsAction.FILE_PATH;
  public static final String DEST = "-dest";
  private String bucketPrefix;
  private int numBuckets;
  private int randomSeed;
  private String srcPath;
  private String destPath;
  private int bufferSize = 64 * 1024;
  private Configuration conf;
  Random rand ;
  @Override
  public void init(Map<String, String> args) {
    try {
      this.conf = getContext().getConf();
      String nameNodeURL =
          this.conf.get(SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY);
      conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, nameNodeURL);
    } catch (NullPointerException e) {
      this.conf = new Configuration();
      appendLog("Conf error!, NameNode URL is not configured!");
    }
    super.init(args);
    this.srcPath = args.get(FILE_PATH);
    this.bucketPrefix = this.conf.get(SmartConfKeys.EVERSPAN_BUCKET_PREFIX_KEY, SmartConfKeys.EVERSPAN_BUCKET_PREFIX_KEY_DEFAULT ) ;
    this.numBuckets = this.conf.getInt(SmartConfKeys.EVERSPAN_PARTITION_NUM_BUCKETS_KEY, SmartConfKeys.EVERSPAN_PARTITION_NUM_BUCKETS_KEY_DEFAULT)  ;
    appendLog( "BucketPrefix -" + this.bucketPrefix );
    appendLog( "Number of buckets -" + this.numBuckets );
    rand = new Random(); 
       
    if (args.containsKey(DEST)) {
    	try {
          if(args.containsKey(USE_SINGLE_BUCKET_PER_SESSION)) {
            Pattern p = Pattern.compile("(session)(\\d+)");
	    Matcher m = p.matcher(this.srcPath);
            String sessionID = "" ; 
	    while (m.find()) {
              sessionID = m.group(2);
	      appendLog("Session ID is " +  sessionID );
	    }        
            if (sessionID == "") {
              throw new IllegalArgumentException("Using single bucket per sessionID - Session ID is missing, check the srcPath - " + this.srcPath);
            }
 
            MessageDigest md = MessageDigest.getInstance("MD5");
	    md.update(sessionID.getBytes());
	    byte[] digest_bytes = md.digest();
	    appendLog("Using single bucket per sessionID - Digest bytes " + digest_bytes);
            this.destPath = getConstBucketName(digest_bytes) + this.srcPath ;   
            appendLog( "In class Copy2S3Action - destination -" + this.destPath );
          }
          else {
            this.destPath = getBucketName() + this.srcPath ;
            appendLog( "In class Copy2S3Action - Using Random order of buckets - destination -" + this.destPath );
           }
        }
	catch(Exception e) {
          appendLog("Conf error!, S3 Bucket doesn't exist! ");
        }
    }
    if (args.containsKey(BUF_SIZE)) {
      bufferSize = Integer.valueOf(args.get(BUF_SIZE));
    }
    
  }

  @Override
  protected void execute() throws Exception {
    if (srcPath == null) {
      throw new IllegalArgumentException("File parameter is missing.");
    }
    if (destPath == null) {
      throw new IllegalArgumentException("Dest File parameter is missing.");
    }
    appendLog(
        String.format("Action starts at %s : Read %s",
            Utils.getFormatedCurrentTime(), srcPath));
    if (!dfsClient.exists(srcPath)) {
      throw new ActionException("CopyFile Action fails, file doesn't exist!");
    }
    appendLog(
        String.format("Copy from %s to %s", srcPath, destPath));
    copySingleFile(srcPath, destPath);
    appendLog(String.format("Successful Copy from %s to %s", srcPath, destPath));
    setXAttribute(srcPath, destPath);
}

    // Everspan Related to achieve greater parallelism of writes

  private String getBucketName() throws IOException {
    int randomNum = rand.nextInt(this.numBuckets) + 1;
    appendLog( "Debug parameters - in class Copy2S3Action - randomNum -" + randomNum );
    return this.bucketPrefix + randomNum  ;
  }

  //hashing the sessionID to distribute the randomBucket and also using the same bucket per session
  private String getConstBucketName(byte[] md5_bytes) throws IOException {
    int temp = md5_bytes[3] + md5_bytes[5] + md5_bytes[7] + md5_bytes[11] + md5_bytes[13];
    appendLog( "Debug parameters - in class Copy2S3Action - getConstBucketName -" + temp );
    return this.bucketPrefix + ((Math.abs((temp % this.numBuckets))) + 1);
  }


  private long getFileSize(String fileName) throws IOException {
    if (fileName.startsWith("hdfs")) {
      // Get InputStream from URL
      FileSystem fs = FileSystem.get(URI.create(fileName), conf);
      return fs.getFileStatus(new Path(fileName)).getLen();
    } else {
      return dfsClient.getFileInfo(fileName).getLen();
    }
  }

  private boolean setXAttribute(String src, String dest) throws IOException {

    String name = "user.coldloc";
    dfsClient.setXAttr(srcPath, name, dest.getBytes(), EnumSet.of(XAttrSetFlag.CREATE,XAttrSetFlag.REPLACE) );
    appendLog(" SetXattr feature is set - srcPath  " + srcPath + "destination" + dest.getBytes() );
    return true; 
  }

  private boolean copySingleFile(String src, String dest) throws IOException {
    //get The file size of source file
    InputStream in = null;
    OutputStream out = null;

    try {
      in = getSrcInputStream(src);
      out = CompatibilityHelperLoader.getHelper().getS3outputStream(dest,conf);
      byte[] buf = new byte[bufferSize];
      long bytesRemaining = getFileSize(src);

      while (bytesRemaining > 0L) {
        int bytesToRead =
            (int) (bytesRemaining < (long) buf.length ? bytesRemaining :
                (long) buf.length);
        int bytesRead = in.read(buf, 0, bytesToRead);
        if (bytesRead == -1) {
          break;
        }
        out.write(buf, 0, bytesRead);
        bytesRemaining -= (long) bytesRead;
      }
      return true;
    } finally {
      if (out != null) {
        out.close();
      }
      if (in != null) {
        in.close();
      }
    }
  }

  private InputStream getSrcInputStream(String src) throws IOException {
    if (!src.startsWith("hdfs")) {
      // Copy between different remote clusters
      // Get InputStream from URL
      FileSystem fs = FileSystem.get(URI.create(src), conf);
      return fs.open(new Path(src));
    } else {
      // Copy from primary HDFS
      return dfsClient.open(src);
    }
  }

  private OutputStream getDestOutPutStream(String dest) throws IOException {
    // Copy to remote S3
    if (!dest.startsWith("s3")) {
      throw new IOException();
    }
    // Copy to s3
    FileSystem fs = FileSystem.get(URI.create(dest), conf);
    return fs.create(new Path(dest), true);
  }
}
