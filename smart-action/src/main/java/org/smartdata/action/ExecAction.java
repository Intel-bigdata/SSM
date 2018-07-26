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
package org.smartdata.action;

import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.utils.StringUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An action to execute general command.
 *
 */
// TODO: Add security restrictions
@ActionSignature(
    actionId = "exec",
    displayName = "exec",
    usage = ExecAction.CMD + " $cmdString"
        + " [" + ExecAction.EXECDIR + " $executionDirectory" + "]"
        + " [" + ExecAction.ENV + " $envKVs" + "]"
)
public class ExecAction extends SmartAction {
  public static final String CMD = "-cmd";
  public static final String EXECDIR = "-execdir";
  public static final String ENV = "-env"; // multi-KVs separated with '|'
  public static final String SSM_ENV_PREFIX = "SSMENV";
  private Map<String, String> env = new HashMap<>();
  private String cmdStr = "";
  private String execDir = "";

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    String key;
    String value;
    for (String arg : args.keySet()) {
      switch (arg) {
        case CMD:
          cmdStr = args.get(arg);
          break;
        case EXECDIR:
          execDir = args.get(arg);
          break;
        case ENV:
          value = args.get(ENV);
          if (value == null || value.length() == 0) {
            break;
          }
          env.putAll(parseEnvString(value));
          break;
        default:
          key = SSM_ENV_PREFIX + (arg.startsWith("-") ? arg.replaceFirst("-", "_") : arg);
          env.put(key, args.get(arg));
      }
    }
  }

  private Map<String, String> parseEnvString(String envStr) {
    String[] items = envStr.split("\\|");
    String key;
    String temp;
    Map<String, String> ret = new HashMap<>();
    for (String it : items) {
      int idx = it.indexOf("=");
      if (idx != -1) {
        key = it.substring(0, idx).trim();
        if (it.length() == idx + 1) {
          temp = "";
        } else {
          temp = it.substring(idx + 1, it.length());
          temp = temp.replaceAll("\\s+$", "");
        }
        if (key.length() > 0) {
          ret.put(key, temp);
        }
      }
    }
    return ret;
  }

  @Override
  protected void execute() throws Exception {
    List<String> cmdItems = StringUtil.parseCmdletString(cmdStr);
    if (cmdItems.size() == 0) {
      return;
    }

    ProcessBuilder builder = new ProcessBuilder(cmdItems);
    if (execDir != null && execDir.length() > 0) {
      builder.directory(new File(execDir));
    }

    Map<String, String> envVars = builder.environment();
    envVars.putAll(env);

    builder.redirectErrorStream(true);
    Process p = builder.start();

    BufferedReader stdout = new BufferedReader(new InputStreamReader(p.getInputStream()));
    String line;
    while ((line = stdout.readLine()) != null) {
      appendLog(line);
    }
    int eCode = p.waitFor();
    if (eCode != 0) {
      throw new IOException("Exit code = " + eCode);
    }
  }
}
