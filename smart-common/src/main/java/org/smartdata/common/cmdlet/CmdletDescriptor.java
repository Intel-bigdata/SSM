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
package org.smartdata.common.cmdlet;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * CmdletDescriptor describes a cmdlet by parsing out action names and their
 * parameters like shell format. It does not including verifications like
 * whether the action name is supported or the parameters are valid or not.
 */
public class CmdletDescriptor {
  private long ruleId;
  private List<String> actionNames = new ArrayList<>();
  private List<String[]> actionArgs = new ArrayList<>();
  private String cmdletString = null;

  private static final String REG_ACTION_NAME = "^[a-zA-Z]+[a-zA-Z0-9_]*";

  public CmdletDescriptor() {
  }

  public CmdletDescriptor(String cmdletString) throws ParseException {
    this(cmdletString, 0);
  }

  public CmdletDescriptor(String cmdletString, long ruleId) throws ParseException {
    this.ruleId = ruleId;
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

  public long getRuleId() {
    return ruleId;
  }

  public void setRuleId(long ruleId) {
    this.ruleId = ruleId;
  }

  public void addAction(String actionName, String[] args) {
    actionNames.add(actionName);
    actionArgs.add(args);
  }

  public String getActionName(int index) {
    return actionNames.get(index);
  }

  public String[] getActionArgs(int index) {
    return actionArgs.get(index);
  }

  public int size() {
    return actionNames.size();
  }

  // TODO: descriptor --> String

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
    if (size() == 0) {
      return "";
    }

    String cmds = getActionName(0) + " "
        + formatActionArguments(getActionArgs(0));
    for (int i = 1; i < size(); i++) {
      cmds += " ; " + getActionName(i) + " "
          + formatActionArguments(getActionArgs(i));
    }
    return cmds;
  }

  public boolean equals(CmdletDescriptor des) {
    if (des == null || this.size() != des.size()) {
      return false;
    }

    for (int i = 0; i < this.size(); i++) {
      if (!actionNames.get(i).equals(des.getActionName(i))
          || actionArgs.get(i).length != des.getActionArgs(i).length) {
        return false;
      }

      String[] srcArgs = actionArgs.get(i);
      String[] destArgs = des.getActionArgs(i);
      for (int j = 0; j < actionArgs.get(i).length; j++) {
        if (!srcArgs[j].equals(destArgs[j])) {
          return false;
        }
      }
    }
    return true;
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
      String tokenString = String.valueOf(token, 0, tokenlen);
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
      throw new ParseException("Invalid action name: " + blocks.get(0), offset);
    }
    String name = blocks.get(0);
    blocks.remove(0);
    String[] args = blocks.toArray(new String[blocks.size()]);
    addAction(name, args);
    blocks.clear();
  }

  private String formatActionArguments(String[] args) {
    // TODO: check more cases / using another way
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
}
