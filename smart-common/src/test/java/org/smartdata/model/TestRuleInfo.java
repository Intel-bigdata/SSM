package org.smartdata.model;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by zhendu on 2017/7/5.
 */
public class TestRuleInfo {
  private class ChildRuleInfo extends RuleInfo {

  }

  @Test
  public void testEquals() throws Exception {
    //Case 1:
    Assert.assertEquals(true,new RuleInfo().equals(new RuleInfo()));

    //Case 2:
    RuleInfo ruleInfo = new RuleInfo(1,1,"",RuleState.ACTIVE , 1,1,1);
    Assert.assertEquals(true , ruleInfo.equals(ruleInfo));

    //Case 3:
    RuleInfo ruleInfo1 = new RuleInfo(1,1,"",null , 1,1,1);
    Assert.assertEquals(false , ruleInfo.equals(ruleInfo1));
    Assert.assertEquals(false , ruleInfo1.equals(ruleInfo));

    //Case 4:
    RuleInfo ruleInfo2 = new RuleInfo(1,1,null,RuleState.ACTIVE , 1,1,1);
    Assert.assertEquals(false , ruleInfo.equals(ruleInfo2));
    Assert.assertEquals(false , ruleInfo2.equals(ruleInfo));
  }
}
