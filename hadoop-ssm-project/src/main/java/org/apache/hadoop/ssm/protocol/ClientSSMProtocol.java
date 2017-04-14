package org.apache.hadoop.ssm.protocol;

public interface ClientSSMProtocol {
  public int add(int para1, int para2);
  public HAServiceStatus getServiceStatus();
}