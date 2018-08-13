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
package org.smartdata.server.engine.cmdlet.agent.messages;

import org.smartdata.server.engine.cmdlet.agent.AgentMaster;
import org.smartdata.server.engine.cmdlet.agent.messages.MasterToAgent.AgentId;

import java.io.Serializable;

/**
 * Messages sent from SmartAgent to {@link AgentMaster}.
 */
public class AgentToMaster {

  public static class RegisterNewAgent implements Serializable {

    private static final long serialVersionUID = -2967492906579132942L;
    private static RegisterNewAgent instance = new RegisterNewAgent();
    private MasterToAgent.AgentId id;

    private RegisterNewAgent() {
      id = new AgentId("Default");
    }

    public static RegisterNewAgent getInstance() {
      return instance;
    }

    public static RegisterNewAgent getInstance(String id) {
      instance = new RegisterNewAgent();
      instance.setId(new AgentId(id));
      return instance;
    }

    public AgentId getId() {
      return id;
    }

    public void setId(AgentId id) {
      this.id = id;
    }
  }

  public static class RegisterAgent implements Serializable {

    private static final long serialVersionUID = 5566241875786339983L;
    private final MasterToAgent.AgentId id;

    public RegisterAgent(MasterToAgent.AgentId id) {
      this.id = id;
    }

    public MasterToAgent.AgentId getId() {
      return id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      RegisterAgent that = (RegisterAgent) o;

      return id.equals(that.id);
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }

    @Override
    public String toString() {
      return "RegisterAgent{ id=" + id + "}";
    }
  }

  public static class ServeReady implements Serializable{
    private static final long serialVersionUID = 6888516209100011658L;
  }

  public static class AlreadyLaunchedTikv implements Serializable {
    private static final long serialVersionUID = 7129253373711332715L;
    private final MasterToAgent.AgentId id;

    public AlreadyLaunchedTikv(MasterToAgent.AgentId id) {
      this.id = id;
    }

    public String toString() {
      return "Agent" + "[" + id.toString() + "]" + " already launched Tikv.";
    }
  }
}
