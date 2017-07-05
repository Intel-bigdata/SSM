package org.smartdata.model;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Random;

/**
 * Created by zhendu on 2017/7/5.
 */
public class TestActionInfo {
  private class ChildTestActionInfo extends ActionInfo {

  }

  @Test
  public void testEquals() throws Exception {
    //Case 1
    Assert.assertEquals(true, new ActionInfo().equals(new ActionInfo()));

    //Case 2
    Random random = new Random();
    ActionInfo actionInfo = new ActionInfo(random.nextLong(), random.nextLong(),
            " ", new HashMap<String, String>(), " ", " ",
            random.nextBoolean(), random.nextLong(), random.nextBoolean(),
            random.nextLong(), random.nextFloat());
    Assert.assertEquals(true, actionInfo.equals(actionInfo));

    //Case 3
    Assert.assertEquals(false, actionInfo.equals(new ChildTestActionInfo()));
    Assert.assertEquals(false, new ChildTestActionInfo().equals(actionInfo));

    //Case4
    Assert.assertEquals(false, actionInfo.equals(null));

    //Case5
    ActionInfo actionInfo1 = new ActionInfo(1, 1,
            "test", new HashMap<String, String>(), "test", "test",
            true, 1, true,
            1, 1.1f);
    ActionInfo actionInfo2 = new ActionInfo(1, 1,
            "test", new HashMap<String, String>(), "test", "test",
            true, 1, true,
            1, 1.1f);
    Assert.assertEquals(true, actionInfo1.equals(actionInfo2));


    //Case6
    actionInfo1 = new ActionInfo(1, 1,
            "test", new HashMap<String, String>(), "test", "test",
            true, 1, true,
            1, 1.1f);
    actionInfo2 = new ActionInfo(1, 1,
            "test", new HashMap<String, String>(), "", "test",
            true, 1, true,
            1, 1.1f);
    Assert.assertEquals(false, actionInfo1.equals(actionInfo2));

    //Case7
    actionInfo1 = new ActionInfo(1, 1,
            "test", new HashMap<String, String>(), null, "test",
            true, 1, true,
            1, 1.1f);
    actionInfo2 = new ActionInfo(1, 1,
            "test", new HashMap<String, String>(), null, "test",
            true, 1, true,
            1, 1.1f);
    Assert.assertEquals(true, actionInfo1.equals(actionInfo2));

    //Case8
    actionInfo1 = new ActionInfo(1, 1,
            "test", new HashMap<String, String>(), null, "test",
            true, 1, true,
            1, 1.1f);
    actionInfo2 = new ActionInfo(1, 1,
            "test", new HashMap<String, String>(), " ", "test",
            true, 1, true,
            1, 1.1f);
    Assert.assertEquals(false, actionInfo1.equals(actionInfo2));

    //Case9
    actionInfo1 = new ActionInfo(1, 1,
            "test", new HashMap<String, String>(), "", "test",
            true, 1, true,
            1, 1.1f);
    actionInfo2 = new ActionInfo(1, 1,
            "test", null, " ", "test",
            true, 1, true,
            1, 1.1f);
    Assert.assertEquals(false, actionInfo1.equals(actionInfo2));
    Assert.assertEquals(false, actionInfo2.equals(actionInfo1));

  }
}
