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
package org.apache.hadoop.ssm.actions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


/**
 * Move to Cache Unit Test
 */
public class TestMoveToCache {

    private static final int DEFAULT_BLOCK_SIZE = 100;
    private static final String REPLICATION_KEY = "3";

    @Test
    public void testMkdir() throws IOException {

        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
        conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
        conf.setStrings(DFSConfigKeys.DFS_REPLICATION_KEY,REPLICATION_KEY);
        MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
        final DFSClient client = cluster.getFileSystem().getClient();

        final DistributedFileSystem dfs = cluster.getFileSystem();
        Path dir = new Path("/fileTestA");
        dfs.mkdirs(dir);

        String[] str = {"/fileTestA"};

        MoveToCache moveToCache = new MoveToCache(client, conf);
        assertEquals(false, moveToCache.isCached(str[0]));

        moveToCache.initial(str).execute();

        assertEquals(true, moveToCache.isCached(str[0]));
    }
}