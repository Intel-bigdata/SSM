/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.hdfs.action;

import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.model.action.FileMovePlan;

import java.net.URI;
import java.util.UUID;

public class TestSchedulePlan {

  @Test
  public void testJsonConvertion() throws Exception {
    URI nn = new URI("hdfs://localhost:8888");
    String file = "/test/foofile";
    FileMovePlan plan = new FileMovePlan(nn, file);
    plan.addPlan(
        1L,
        UUID.randomUUID().toString(),
        "ARCHIVE",
        "127.0.0.1",
        10001,
       "SSD");
    plan.addPlan(
        2L,
        UUID.randomUUID().toString(),
        "ARCHIVE",
        "127.0.0.1",
        10002,
        "SSD");

    Gson gson = new Gson();
    String jsonPlan = gson.toJson(plan);

    FileMovePlan plan2 = gson.fromJson(jsonPlan, FileMovePlan.class);
    Assert.assertEquals(plan.getFileName(), plan2.getFileName());
    Assert.assertEquals(plan.getBlockIds(), plan2.getBlockIds());
  }
}
