package org.apache.hadoop.ssm.sql;

import org.junit.Test;
import java.sql.Connection;
import java.util.Map;

public class TestGetAccessCount {
  @Test
  public void testGetAccessCount() throws Exception {
    Connection conn = new TestDBUtil().getTestDBInstance();
    DBAdapter dbAdapter = new DBAdapter(conn);
    Map<Long, Integer> ret = dbAdapter.getAccessCount(
      1490932740000l, 1490936400000l, null);
    for (Map.Entry<Long, Integer> entry : ret.entrySet()) {
      System.out.println("fid = " + entry.getKey() +
        ",count = " + entry.getValue());
    }
    if (conn != null) {
      conn.close();
    }
  }
}
