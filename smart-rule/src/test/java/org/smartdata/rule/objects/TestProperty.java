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
package org.smartdata.rule.objects;

import org.junit.Assert;
import org.junit.Test;
import org.smartdata.rule.parser.ValueType;

import java.util.Arrays;

/**
 * Tests to test Property.
 */
public class TestProperty {
  @Test
  public void testEqual() {
    Property p1 = new Property("test", ValueType.LONG,
        Arrays.asList(ValueType.TIMEINTVAL),
        "access_count_table", "", false);

    Property p2 = new Property("test", ValueType.LONG,
        Arrays.asList(ValueType.LONG),
        "access_count_table", "", false);

    Property p3 = new Property("test", ValueType.LONG,
        Arrays.asList(ValueType.TIMEINTVAL),
        "access_count_table", "", false);

    Property p4 = new Property("test", ValueType.TIMEINTVAL,
        Arrays.asList(ValueType.TIMEINTVAL),
        "access_count_table", "", false);

    Property p5 = new Property("test", ValueType.LONG,
        null,
        "access_count_table", "", false);

    Assert.assertTrue(!p1.equals(p2));
    Assert.assertTrue(p1.equals(p3));
    Assert.assertTrue(!p1.equals(p4));
    Assert.assertTrue(!p1.equals(p5));
  }
}
