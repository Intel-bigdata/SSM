package org.smartdata.server.actions;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class TestActionRegister {

//  @Test
//  public void testNativeClassMap() throws Exception {
//    ActionRegister ar = ActionRegister.getInstance();
//    ar.loadNativeAction();
//    String[] actionNames = ar.namesofAction();
//    Assert.assertTrue(actionNames.length == 2);
//  }

  @Test
  public void testUDClassMap() throws Exception {
    ActionRegister ar = ActionRegister.getInstance();
    ar.loadUserDefinedAction();
    String[] actionNames = ar.namesofAction();
    Assert.assertTrue(actionNames.length == 1);
    Action ac = ar.newActionFromName("UDAction");
    ac.run();
  }
}
