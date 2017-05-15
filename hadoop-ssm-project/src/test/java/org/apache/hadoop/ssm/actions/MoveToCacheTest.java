package org.apache.hadoop.ssm.actions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


/**
 * Created by cc on 17-1-12.
 */
public class MoveToCacheTest {

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
        MoveToCache moveToCache= MoveToCache.getInstance(client,conf);

        assertEquals(false,moveToCache.isCached(str[0]));

        moveToCache.initial(str);
        moveToCache.execute();

        assertEquals(true,moveToCache.isCached(str[0]));
    }
}