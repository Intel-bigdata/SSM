package org.smartdata.server;

import org.smartdata.SmartContext;
import org.smartdata.conf.SmartConf;
import org.smartdata.server.metastore.DBAdapter;

public class ServerContext extends SmartContext {

  private DBAdapter dbAdapter;

  public ServerContext(DBAdapter dbAdapter) {
    this.dbAdapter = dbAdapter;
  }

  public ServerContext(SmartConf conf, DBAdapter dbAdapter) {
    super(conf);
    this.dbAdapter = dbAdapter;
  }

  public DBAdapter getDbAdapter() {
    return dbAdapter;
  }
}
