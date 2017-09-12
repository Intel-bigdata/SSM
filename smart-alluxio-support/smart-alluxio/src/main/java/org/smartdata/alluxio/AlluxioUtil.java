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
package org.smartdata.alluxio;

import java.io.IOException;

import org.smartdata.SmartContext;
import org.smartdata.conf.SmartConfKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;

/**
 * Contain utils related to alluxio cluster.
 */
public class AlluxioUtil {

  public static final Logger LOG =
      LoggerFactory.getLogger(AlluxioUtil.class);

  public static FileSystem getAlluxioFs(SmartContext context) throws IOException {
    String alluxioMaster = context.getConf().get(
        SmartConfKeys.SMART_ALLUXIO_MASTER_HOSTNAME_KEY, "localhost");
    Configuration.set(PropertyKey.MASTER_HOSTNAME, alluxioMaster);  
    FileSystemContext fsContext = FileSystemContext.create();
    return FileSystem.Factory.get(fsContext);  
  }
  
 
}
