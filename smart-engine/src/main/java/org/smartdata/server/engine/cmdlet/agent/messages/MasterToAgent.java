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
package org.smartdata.server.engine.cmdlet.agent.messages;

import java.io.Serializable;

public class MasterToAgent {

  public static class AgentId implements scala.Serializable {

    private static final long serialVersionUID = -4032231012646281770L;
    private final String id;

    public AgentId(String id) {
      this.id = id;
    }

    public String getId() {
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

      AgentId agentId = (AgentId) o;

      return id.equals(agentId.id);
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }

    @Override
    public String toString() {
      return "AgentId{id=" + id + "}";
    }
  }

  public static class AgentRegistered implements Serializable {

    private static final long serialVersionUID = -7212238600261028430L;
    private final AgentId id;

    public AgentRegistered(AgentId id) {
      this.id = id;
    }

    public AgentId getAgentId() {
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

      AgentRegistered that = (AgentRegistered) o;

      return id.equals(that.id);
    }

    @Override
    public int hashCode() {
      return id != null ? id.hashCode() : 0;
    }

    @Override
    public String toString() {
      return "AgentRegistered{id=" + id + "}";
    }
  }

  public static class ReadyToLaunchTikv implements Serializable {
    private static final long serialVersionUID = 1927201465466654616L;
  }
}
