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
package org.smartdata.actions.alluxio;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.actions.SmartAction;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;

public abstract class AlluxioAction extends SmartAction {
  protected static final Logger LOG = LoggerFactory.getLogger(AlluxioAction.class);
  public static final String FILE_PATH = "-path";

  protected AlluxioURI uri;
  protected AlluxioActionType actionType;
  protected FileSystem alluxioFs;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.uri = new AlluxioURI(args.get(FILE_PATH));
    this.alluxioFs = FileSystem.Factory.get();
  }

}
