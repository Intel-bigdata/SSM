package org.smartdata.server.engine;

import org.smartdata.conf.ReconfigurableBase;
import org.smartdata.conf.ReconfigureException;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;

import java.util.Arrays;
import java.util.List;

public class ConfManager extends ReconfigurableBase {

  private SmartConf conf;

  public ConfManager(SmartConf conf) {
    this.conf = conf;
  }

  @Override
  public void reconfigureProperty(String property, String newVal)
      throws ReconfigureException {
    if (property.equals(SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY)) {
      conf.set(property, newVal);
    }
  }

  @Override
  public List<String> getReconfigurableProperties() {
    return Arrays.asList(
        SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY
    );
  }
}
