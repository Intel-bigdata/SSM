package org.apache.hadoop.ssm.protocol;

public class HAServiceStatus {
  private HAServiceState state;
  private boolean isActive;

  public HAServiceStatus() {
  }

  public HAServiceStatus(HAServiceState state) {
    this.state = state;
  }

  public HAServiceState getState() {
    return state;
  }

  public void setisActive(boolean b) {
    this.isActive = b;
  }

  public boolean getisActive(){
    return isActive;
  }

  @Override
  public String toString(){
      return "state: "+state.name+"\nisActive: "+isActive;

  }

  public enum HAServiceState {
    SAFEMODE("safemode"),
    ACTIVE("active");

    private String name;

    HAServiceState(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

}