/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.conf;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestReconfigurable {
  public static final String PROPERTY1 = "property1";
  public static final String PROPERTY2 = "property2";

  private class TestReconf extends ReconfigurableBase {
    private String value1 = "oldValue1";
    private String value2 = "oldValue2";

    @Override
    public void reconfigureProperty(String property, String newVal)
        throws ReconfigureException {
      if (property.equals(PROPERTY1) && !newVal.equals(this.value1)) {
        this.value1 = newVal;
      }
    }

    @Override
    public List<String> getReconfigurableProperties() {
      return Arrays.asList(PROPERTY1);
    }

    public String getValue1() {
      return value1;
    }

    public String getValue2() {
      return value2;
    }
  }

  @Test
  public void testReconf() throws Exception {
    Assert.assertEquals(0,
        ReconfigurableRegistry.getAllReconfigurableProperties().size());

    TestReconf reconf = new TestReconf();

    Assert.assertEquals(1,
        ReconfigurableRegistry.getAllReconfigurableProperties().size());

    ReconfigurableRegistry.applyReconfigurablePropertyValue(
        PROPERTY1, "newValue1");
    ReconfigurableRegistry.applyReconfigurablePropertyValue(
        PROPERTY2, "newValue2");

    Assert.assertEquals("newValue1", reconf.getValue1());
    Assert.assertEquals("oldValue2", reconf.getValue2());
  }
}
