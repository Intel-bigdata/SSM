package org.smartdata.model;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;


/**
 * Created by zhendu on 2017/7/5.
 */
public class TestCmdletInfo {
  private class cmdletInfo extends TestCmdletInfo {

  }

  @Test
  public void testEquals() throws Exception {
    //Case 1
    Assert.assertEquals(true, new CmdletInfo().equals(new CmdletInfo()));

    //Case 2
    Random random = new Random();
    CmdletInfo cmdletInfo = new CmdletInfo(random.nextLong(), random.nextLong(), CmdletState.NOTINITED, " ", random.nextLong(), random.nextLong());
    Assert.assertEquals(true, cmdletInfo.equals(cmdletInfo));

    //Case 3
    Assert.assertEquals(false, cmdletInfo.equals(new cmdletInfo()));
    Assert.assertEquals(false, new cmdletInfo().equals(cmdletInfo));

    //Case 4
    Assert.assertEquals(false, cmdletInfo.equals(null));

    //Case 5
    CmdletInfo cmdletInfo1 = new CmdletInfo(1, 1, CmdletState.CANCELLED, "test", 1, 1);
    CmdletInfo cmdletInfo2 = new CmdletInfo(1, 1, CmdletState.CANCELLED, "test", 1, 1);
    Assert.assertEquals(true, cmdletInfo1.equals(cmdletInfo2));

    //Case 6
    cmdletInfo1 = new CmdletInfo(1, 1, CmdletState.CANCELLED, null, 1, 1);
    cmdletInfo2 = new CmdletInfo(1, 1, CmdletState.CANCELLED, "test", 1, 1);
    Assert.assertEquals(false, cmdletInfo1.equals(cmdletInfo2));
    Assert.assertEquals(false, cmdletInfo2.equals(cmdletInfo1));

    //Case 7
    cmdletInfo1 = new CmdletInfo(1, 1, CmdletState.CANCELLED, null, 1, 1);
    cmdletInfo2 = new CmdletInfo(1, 1, CmdletState.CANCELLED, null, 1, 1);
    Assert.assertEquals(true, cmdletInfo1.equals(cmdletInfo2));

    //Case 8
    cmdletInfo1 = new CmdletInfo(1, 1, null, "test", 1, 1);
    cmdletInfo2 = new CmdletInfo(1, 1, CmdletState.CANCELLED, "test", 1, 1);
    Assert.assertEquals(false, cmdletInfo1.equals(cmdletInfo2));

    //Case 9
    cmdletInfo1 = new CmdletInfo(1, 1, null, "test", 1, 1);
    cmdletInfo2 = new CmdletInfo(1, 1, null, "test", 1, 1);
    Assert.assertEquals(true, cmdletInfo1.equals(cmdletInfo2));
  }
}
