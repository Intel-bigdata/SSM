package org.smartdata.server;

import org.smartdata.SmartContext;
import org.smartdata.conf.SmartConf;
import org.smartdata.server.metastore.MetaStore;

public class ServerContext extends SmartContext {

  private MetaStore metaStore;

  public ServerContext(MetaStore metaStore) {
    this.metaStore = metaStore;
  }

  public ServerContext(SmartConf conf, MetaStore metaStore) {
    super(conf);
    this.metaStore = metaStore;
  }

  public MetaStore getMetaStore() {
    return metaStore;
  }
}
