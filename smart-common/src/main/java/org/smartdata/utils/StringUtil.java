/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.smartdata.utils;

import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {
  private static final String DIR_SEP = "/";
  private static final String[] GLOBS = new String[] {
      "*", "?"
  };

  public static String join(CharSequence delimiter,
      Iterable<? extends CharSequence> elements) {
    if (elements == null) {
      return null;
    }

    StringBuilder sb = new StringBuilder();
    Iterator<? extends CharSequence> it = elements.iterator();
    if (!it.hasNext()) {
      return "";
    }

    sb.append(it.next());

    while (it.hasNext()) {
      sb.append(delimiter);
      sb.append(it.next());
    }
    return sb.toString();
  }

  public static String getBaseDir(String path) {
    if (path == null) {
      return null;
    }

    int last = path.lastIndexOf(DIR_SEP);
    if (last == -1) {
      return null;
    }

    int first = path.length();
    for (String g : GLOBS) {
      int gIdx = path.indexOf(g);
      if (gIdx >= 0) {
        first = gIdx < first ? gIdx : first;
      }
    }

    last = path.substring(0, first).lastIndexOf(DIR_SEP);
    if (last == -1) {
      return null;
    }
    return path.substring(0, last + 1);
  }

  public static String globString2SqlLike(String str) {
    str = str.replace("*", "%");
    str = str.replace("?", "_");
    return str;
  }

  /**
   * Convert time string into milliseconds representation.
   *
   * @param str
   * @return -1 if error
   */
  public static long pharseTimeString(String str) {
    long intval = 0L;
    str = str.trim();
    Pattern p = Pattern.compile("([0-9]+)([a-z]+)");
    Matcher m = p.matcher(str);
    int start = 0;
    while (m.find(start)) {
      String digStr = m.group(1);
      String unitStr = m.group(2);
      long value = 0;
      try {
        value = Long.parseLong(digStr);
      } catch (NumberFormatException e) {
      }

      switch (unitStr) {
        case "d":
        case "day":
          intval += value * 24 * 3600 * 1000;
          break;
        case "h":
        case "hour":
          intval += value * 3600 * 1000;
          break;
        case "m":
        case "min":
          intval += value * 60 * 1000;
          break;
        case "s":
        case "sec":
          intval += value * 1000;
          break;
        case "ms":
          intval += value;
          break;
      }
      start += m.group().length();
    }
    return intval;
  }
}
