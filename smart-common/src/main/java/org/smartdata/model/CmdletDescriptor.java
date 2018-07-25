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
package org.smartdata.model;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CmdletDescriptor describes a cmdlet by parsing out action names and their
 * parameters like shell format. It does not including verifications like
 * whether the action is supported or the parameters are valid or not.
 *
 * <p>Cmdlet string should have the following format:
 *    action1 [-option [value]] ... [; action2 [-option [value]] ...]
 */
public class CmdletDescriptor {
  public static final String RULE_ID = "-ruleId";
  public static final String HDFS_FILE_PATH = "-file";

  private Map<String, String> actionCommon = new HashMap<>();
  private List<String> actionNames = new ArrayList<>();
  private List<Map<String, String>> actionArgs = new ArrayList<>();
  private String cmdletString = null;

  private static final String REG_ACTION_NAME = "^[a-zA-Z]+[a-zA-Z0-9_]*";

  public CmdletDescriptor() {
  }

  public CmdletDescriptor(String cmdletString) throws ParseException {
    setCmdletString(cmdletString);
  }

  public CmdletDescriptor(String cmdletString, long ruleId)
      throws ParseException {
    setRuleId(ruleId);
    setCmdletString(cmdletString);
  }

  public String getCmdletString() {
    return cmdletString == null ? toCmdletString() : cmdletString;
  }

  public void setCmdletString(String cmdletString) throws ParseException {
    actionNames.clear();
    actionArgs.clear();
    this.cmdletString = cmdletString;
    parseCmdletString(cmdletString);
  }

  public void setCmdletParameter(String key, String value) {
    actionCommon.put(key, value);
  }

  public String getCmdletParameter(String key) {
    return actionCommon.get(key);
  }

  public long getRuleId() {
    String idStr = actionCommon.get(RULE_ID);
    try {
      return idStr == null ? 0 : Long.valueOf(idStr);
    } catch (Exception e) {
      return 0;
    }
  }

  public void setRuleId(long ruleId) {
    actionCommon.put(RULE_ID, "" + ruleId);
  }

  public void addAction(String actionName, Map<String, String> args) {
    actionNames.add(actionName);
    actionArgs.add(args);
  }

  public List<String> getActionNames() {
    List<String> ret = new ArrayList<>(actionNames.size());
    ret.addAll(actionNames);
    return ret;
  }

  public String getActionName(int index) {
    return actionNames.get(index);
  }

  /**
   * Get a complete set of arguments including cmdlet common part.
   * @param index
   * @return
   */
  public Map<String, String> getActionArgs(int index) {
    Map<String, String> map = new HashMap<>();
    map.putAll(actionCommon);
    map.putAll(actionArgs.get(index));
    return map;
  }

  public void addActionArg(int index, String key, String value) {
    actionArgs.get(index).put(key, value);
  }

  public String deleteActionArg(int index, String key) {
    Map<String, String> args = actionArgs.get(index);
    String value = null;
    if (args.containsKey(key)) {
      value = args.get(key);
      args.remove(key);
    }
    return value;
  }

  public int getActionSize() {
    return actionNames.size();
  }

  /**
   * Construct an CmdletDescriptor from cmdlet string.
   * @param cmdString
   * @return
   * @throws ParseException
   */
  public static CmdletDescriptor fromCmdletString(String cmdString)
      throws ParseException {
    CmdletDescriptor des = new CmdletDescriptor(cmdString);
    return des;
  }

  public String toCmdletString() {
    if (getActionSize() == 0) {
      return "";
    }

    String cmds = getActionName(0) + " "
        + formatActionArguments(getActionArgs(0));
    for (int i = 1; i < getActionSize(); i++) {
      cmds += " ; " + getActionName(i) + " "
          + formatActionArguments(getActionArgs(i));
    }
    return cmds;
  }

  public boolean equals(CmdletDescriptor des) {
    if (des == null || this.getActionSize() != des.getActionSize()) {
      return false;
    }

    for (int i = 0; i < this.getActionSize(); i++) {
      if (!actionNames.get(i).equals(des.getActionName(i))) {
        return false;
      }

      Map<String, String> srcArgs = getActionArgs(i);
      Map<String, String> destArgs = des.getActionArgs(i);

      if (srcArgs.size() != destArgs.size()) {
        return false;
      }

      for (String key : srcArgs.keySet()) {
        if (!srcArgs.get(key).equals(destArgs.get(key))) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return String.format(
        "CmdletDescriptor{actionCommon=%s, actionNames=%s, "
            + "actionArgs=%s, cmdletString=\'%s\'}",
        actionCommon, actionNames, actionArgs, cmdletString);
  }

  private void parseCmdletString(String cmdlet)
      throws ParseException {
    if (cmdlet == null || cmdlet.length() == 0) {
      return;
    }

    char[] chars = (cmdlet + " ").toCharArray();
    List<String> blocks = new ArrayList<String>();
    char c;
    char[] token = new char[chars.length];
    int tokenlen = 0;
    boolean sucing = false;
    boolean string = false;
    for (int idx = 0; idx < chars.length; idx++) {
      c = chars[idx];
      if (c == ' ' || c == '\t') {
        if (string) {
          token[tokenlen++] = c;
        }
        if (sucing) {
          blocks.add(String.valueOf(token, 0, tokenlen));
          tokenlen = 0;
          sucing = false;
        }
      } else if (c == ';') {
        if (string) {
          throw new ParseException("Unexpected break of string", idx);
        }

        if (sucing) {
          blocks.add(String.valueOf(token, 0, tokenlen));
          tokenlen = 0;
          sucing = false;
        }

        verify(blocks, idx);
      } else if (c == '\\') {
        boolean tempAdded = false;
        if (sucing || string) {
          token[tokenlen++] = chars[++idx];
          tempAdded = true;
        }

        if (!tempAdded && !sucing) {
          sucing = true;
          token[tokenlen++] = chars[++idx];
        }
      } else if (c == '"') {
        if (sucing) {
          throw new ParseException("Unexpected \"", idx);
        }

        if (string) {
          if (chars[idx + 1] != '"') {
            string = false;
            blocks.add(String.valueOf(token, 0, tokenlen));
            tokenlen = 0;
          } else {
            idx++;
          }
        } else {
          string = true;
        }
      } else if (c == '\r' || c == '\n') {
        if (sucing) {
          sucing = false;
          blocks.add(String.valueOf(token, 0, tokenlen));
          tokenlen = 0;
        }

        if (string) {
          throw new ParseException("String cannot in more than one line", idx);
        }
      } else {
        if (string) {
          token[tokenlen++] = chars[idx];
        } else {
          sucing = true;
          token[tokenlen++] = chars[idx];
        }
      }
    }

    if (string) {
      throw new ParseException("Unexpect tail of string", chars.length);
    }

    if (blocks.size() > 0) {
      verify(blocks, chars.length);
    }
  }

  private void verify(List<String> blocks, int offset) throws ParseException {
    if (blocks.size() == 0) {
      throw new ParseException("Contains NULL action", offset);
    }

    boolean matched = blocks.get(0).matches(REG_ACTION_NAME);
    if (!matched) {
      throw new ParseException("Invalid action name: "
          + blocks.get(0), offset);
    }
    String name = blocks.get(0);
    blocks.remove(0);
    Map<String, String> args = toArgMap(blocks);
    addAction(name, args);
    blocks.clear();
  }

  public static List<String> toArgList(Map<String, String> args) {
    List<String> ret = new ArrayList<>();
    for (String key : args.keySet()) {
      ret.add(key);
      ret.add(args.get(key));
    }
    return ret;
  }

  public static Map<String, String> toArgMap(List<String> args)
      throws ParseException {
    Map<String, String> ret = new HashMap<>();
    String opt = null;
    for (int i = 0; i < args.size(); i++) {
      String arg = args.get(i);
      if (arg.startsWith("-")) {
        if (opt == null) {
          opt = arg;
        } else {
          ret.put(opt, "");
          opt = arg;
        }
      } else {
        if (opt == null) {
          throw new ParseException("Unknown action option name for value '"
              + arg + "'", 0);
        } else {
          ret.put(opt, arg);
          opt = null;
        }
      }
    }
    if (opt != null) {
      ret.put(opt, "");
    }
    return ret;
  }

  private String formatActionArguments(String[] args) {
    if (args == null || args.length == 0) {
      return "";
    }

    String ret = "";
    for (String arg : args) {
      String rep = arg.replace("\\", "\\\\");
      rep = rep.replace("\"", "\\\"");
      if (rep.matches(".*\\s+.*")) {
        ret += " \"" + rep + "\"";
      } else {
        ret += " " + rep;
      }
    }
    return ret.trim();
  }

  private String formatActionArguments(Map<String, String> args) {
    if (args == null || args.size() == 0) {
      return "";
    }

    String ret = "";
    for (String key : args.keySet()) {
      ret += " " + formatItems(key) + " " + formatItems(args.get(key));
    }
    return ret.trim();
  }

  private String formatItems(String arg) {
    String rep = arg.replace("\\", "\\\\");
    rep = rep.replace("\"", "\\\"");
    if (rep.matches(".*\\s+.*")) {
      return "\"" + rep + "\"";
    } else {
      return rep;
    }
  }
}
