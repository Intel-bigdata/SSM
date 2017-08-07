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
package org.smartdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.IOException;

public abstract class MiniClusterFactory {

  static class DefaultMiniClusterFactory extends MiniClusterFactory {
    @Override
    public MiniDFSCluster create(int dataNodes, Configuration conf) throws IOException {
      return new MiniDFSCluster.Builder(conf).numDataNodes(dataNodes).build();
    }

    @Override
    public MiniDFSCluster createWithStorages(int dataNodes, Configuration conf) throws IOException {
      throw new UnsupportedOperationException(
          "DefaultMiniClusterFactory does not support creating cluster with storage types");
    }
  }

  //Todo: Use better init implementation
  public static MiniClusterFactory get()
      throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    Class clazz = null;
    try {
      clazz =
          Thread.currentThread()
              .getContextClassLoader()
              .loadClass("org.smartdata.hdfs.MiniClusterFactory27");
    } catch (ClassNotFoundException e) {
      try {
        clazz =
            Thread.currentThread()
                .getContextClassLoader()
                .loadClass("org.smartdata.hdfs.MiniClusterFactory26");
      } catch (ClassNotFoundException e1) {
      }
    }
    return clazz == null
        ? new DefaultMiniClusterFactory()
        : (MiniClusterFactory) clazz.newInstance();
  }

  public abstract MiniDFSCluster create(int dataNodes, Configuration conf) throws IOException;

  public abstract MiniDFSCluster createWithStorages(int dataNodes, Configuration conf)
      throws IOException;
}
