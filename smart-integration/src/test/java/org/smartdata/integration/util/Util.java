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
package org.smartdata.integration.util;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import org.apache.commons.lang.StringUtils;
import org.smartdata.agent.SmartAgent;
import org.smartdata.integration.rest.RestApiBase;
import org.smartdata.server.SmartDaemon;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.restassured.path.json.JsonPath.with;

public class Util {

  public static void waitSlaveServerAvailable() throws InterruptedException {
    Util.retryUntil(new RetryTask() {
      @Override
      public boolean retry() {
        Response response = RestAssured.get(RestApiBase.SYSTEMROOT + "/servers");
        List<String> ids = with(response.asString()).get("body.id");
        return ids.size() > 0;
      }
    }, 15);
  }

  public static void waitSlaveServersDown() throws InterruptedException {
    Util.retryUntil(new RetryTask() {
      @Override
      public boolean retry() {
        Response response = RestAssured.get(RestApiBase.SYSTEMROOT + "/servers");
        List<String> ids = with(response.asString()).get("body.id");
        return ids.size() == 0;
      }
    }, 15);
  }

  public static void waitAgentAvailable() throws InterruptedException {
    Util.retryUntil(new RetryTask() {
      @Override
      public boolean retry() {
        Response response = RestAssured.get(RestApiBase.SYSTEMROOT + "/agents");
        List<String> ids = with(response.asString()).get("body.id");
        return ids.size() > 0;
      }
    }, 15);
  }

  public static void waitAgentsDown() throws InterruptedException {
    Util.retryUntil(new RetryTask() {
      @Override
      public boolean retry() {
        Response response = RestAssured.get(RestApiBase.SYSTEMROOT + "/agents");
        List<String> ids = with(response.asString()).get("body.id");
        return ids.size() == 0;
      }
    }, 15);
  }

  public static void retryUntil(RetryTask retryTask, int maxRetries) throws InterruptedException {
    retryUntil(retryTask, maxRetries, 1000);
  }

  public static void retryUntil(RetryTask retryTask, int maxRetries, long interval)
      throws InterruptedException {
    boolean met = false;
    int retries = 0;

    while (!met && retries < maxRetries) {
      met = retryTask.retry();
      retries += 1;
      if (!met) {
        Thread.sleep(interval);
      }
    }

    if (!met) {
      throw new RuntimeException("Failed after retry " + maxRetries + "times.");
    }
  }

  public static Process startNewServer() throws IOException {
    return Util.buildProcess(
        System.getProperty("java.class.path").split(java.io.File.pathSeparator),
        SmartDaemon.class.getCanonicalName(),
        new String[0]);
  }

  public static Process startNewAgent() throws IOException {
    return Util.buildProcess(
        System.getProperty("java.class.path").split(java.io.File.pathSeparator),
        SmartAgent.class.getCanonicalName(),
        new String[0]);
  }

  public static Process buildProcess(
      String[] options, String[] classPath, String mainClass, String[] arguments)
      throws IOException {
    String java = System.getProperty("java.home") + "/bin/java";
    List<String> commands = new ArrayList<>();
    commands.add(java);
    commands.addAll(Arrays.asList(options));
    commands.add("-cp");
    commands.add(StringUtils.join(classPath, File.pathSeparator));
    commands.add(mainClass);
    commands.addAll(Arrays.asList(arguments));
    return new ProcessBuilder(commands).start();
  }

  public static Process buildProcess(String mainClass, String[] arguments) throws IOException {
    return buildProcess(new String[0], mainClass, arguments);
  }

  public static Process buildProcess(String[] classPath, String mainClass, String[] arguments)
      throws IOException {
    return buildProcess(new String[0], classPath, mainClass, arguments);
  }
}
