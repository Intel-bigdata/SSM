package org.apache.hadoop.ssm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;

import java.io.IOException;
import java.util.Map;

/**
 * Created by cc on 17-2-28.
 */
public class CacheInfo {
  private String CachePoolName;
  private String filePath;
  private int replication;
  private Map<String, Integer> map;


  Configuration conf = new Configuration();
  DFSClient dfsClient = new DFSClient(conf);


  public CacheInfo() throws IOException {
  }

  public void asd(){



  }
}
