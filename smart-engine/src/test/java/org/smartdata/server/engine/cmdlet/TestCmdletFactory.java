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
package org.smartdata.server.engine.cmdlet;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.smartdata.SmartContext;
import org.smartdata.action.ActionException;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.model.LaunchAction;

import java.util.HashMap;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCmdletFactory {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testCreateAction() throws ActionException {
    SmartContext smartContext = mock(SmartContext.class);
    SmartConf conf = new SmartConf();
    conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY, "http://0.0.0.0:8088");
    conf.set(SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY, "hdfs://0.0.0.0:8089");
    when(smartContext.getConf()).thenReturn(conf);
    CmdletFactory cmdletFactory = new CmdletFactory(smartContext);

    LaunchAction launchAction1 = new LaunchAction(10, "allssd", new HashMap<String, String>());
//    SmartAction action = cmdletFactory.createAction(launchAction1);
//    Assert.assertNotNull(action);
//    Assert.assertEquals(10, action.getActionId());
//
//    LaunchAction launchAction = new LaunchAction(10, "test", new HashMap<String, String>());
//    expectedException.expect(ActionException.class);
//    Assert.assertNull(cmdletFactory.createAction(launchAction));
  }
}
