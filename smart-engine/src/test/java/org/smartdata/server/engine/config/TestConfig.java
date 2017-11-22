package org.smartdata.server.engine.config;

import org.smartdata.server.cluster.HazelcastInstanceProvider;

public class TestConfig {

  public static void main(String[] args) {
    HazelcastInstanceProvider.getInstance();
  }

}
