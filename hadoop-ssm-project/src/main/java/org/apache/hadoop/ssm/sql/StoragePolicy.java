package org.apache.hadoop.ssm.sql;

public class StoragePolicy {
  private byte sid;
  private String policyName;

  public StoragePolicy(byte sid, String policyName) {
    this.sid = sid;
    this.policyName = policyName;
  }

  public byte getSid() {
    return sid;
  }
  public void setSid(byte sid) {
    this.sid = sid;
  }
  public String getPolicyName() {
    return policyName;
  }
  public void setPolicyName(String policyName) {
    this.policyName = policyName;
  }
}
