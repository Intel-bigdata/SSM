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
package org.smartdata.server.rule.objects;

import org.junit.Assert;
import org.junit.Test;
import org.smartdata.server.rule.parser.ValueType;

import java.util.Arrays;
import java.util.List;

/**
 * Tests to test PropertyRealParas.
 */
public class TestPropertyRealParas {

  @Test
  public void testEqual() {
    Property p = new Property("test", ValueType.LONG,
        Arrays.asList(ValueType.LONG),
        "test", "", false);

    List<Object> v1 = Arrays.asList((Object) new Long(1));
    List<Object> v2 = Arrays.asList((Object) new Integer(1));
    List<Object> v3 = Arrays.asList((Object) new String("1"));
    List<Object> v4 = Arrays.asList((Object) new Long(100));
    List<Object> v5 = Arrays.asList((Object) new Long(1));

    PropertyRealParas rp1 = new PropertyRealParas(p, v1);
    PropertyRealParas rp2 = new PropertyRealParas(p, v2);
    PropertyRealParas rp3 = new PropertyRealParas(p, v3);
    PropertyRealParas rp4 = new PropertyRealParas(p, v4);
    PropertyRealParas rp5 = new PropertyRealParas(p, v5);
    PropertyRealParas rp6 = new PropertyRealParas(p, null);

    Assert.assertFalse(rp1.equals(rp2));
    Assert.assertFalse(rp1.equals(rp3));
    Assert.assertFalse(rp1.equals(rp4));
    Assert.assertFalse(!rp1.equals(rp5));
    Assert.assertFalse(rp1.equals(rp6));
  }
}
