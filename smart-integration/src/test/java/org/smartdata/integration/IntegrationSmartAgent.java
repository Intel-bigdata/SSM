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
package org.smartdata.integration;

import org.smartdata.agent.SmartAgent;

import java.io.IOException;

public class IntegrationSmartAgent {
  private AgentRunner agent;

  public void setup() throws IOException{
    this.agent = new AgentRunner();
    agent.start();
  }

  public void close() {
    this.agent.close();
  }

  private class AgentRunner extends Thread {
    private SmartAgent agent;
    @Override
    public void run() {
      agent = new SmartAgent();
      try {
        agent.main(null);
      } catch (IOException ex) {
        System.out.println(ex.getMessage());
      }
    }

    public void close() {
      agent.close();
    }
  }
}
