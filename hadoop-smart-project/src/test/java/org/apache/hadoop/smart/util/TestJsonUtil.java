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
package org.apache.hadoop.smart.util;


import org.apache.hadoop.smart.utils.JsonUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestJsonUtil {

  @Test
  public void testTransitionBetweenMapAndString() throws Exception {
    Map<String, String> mapParams = new HashMap<>();
    mapParams.put("id", "avcde@#$%^^&~!@#$%^&*()3,./;'[]\\<>?:\"{}|\"");
    mapParams.put("k:[{'", "1024");
    String jsonString = JsonUtil.toJsonString(mapParams);

    Map<String, String> mapRevert = JsonUtil.toStringStringMap(jsonString);
    Assert.assertTrue(mapParams.size() == mapRevert.size());
    for (String key : mapRevert.keySet()) {
      Assert.assertTrue(mapParams.get(key).equals(mapRevert.get(key)));
    }
  }
}
