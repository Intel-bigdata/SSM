/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.agent.messages;

import org.smartdata.agent.SmartAgent;
import org.smartdata.agent.AgentMaster;

import java.io.Serializable;

/**
 * Messages sent from {@link SmartAgent} to {@link AgentMaster}
 */
public class AgentToMaster {

  public static class RegisterNewAgent implements Serializable {

    private static final long serialVersionUID = -2967492906579132942L;
    private static final RegisterNewAgent instance = new RegisterNewAgent();

    private RegisterNewAgent() {}

    public static RegisterNewAgent getInstance() {
      return instance;
    }
  }

  public static class RegisterAgent implements Serializable {

    private static final long serialVersionUID = 5566241875786339983L;
    private final SmartAgent.AgentId id;

    public RegisterAgent(SmartAgent.AgentId id) {
      this.id = id;
    }

    public SmartAgent.AgentId getId() {
      return id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RegisterAgent that = (RegisterAgent) o;

      return id.equals(that.id);
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }

    @Override
    public String toString() {
      return "RegisterAgent{" +
          "id=" + id +
          '}';
    }
  }
}
