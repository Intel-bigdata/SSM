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

public class StringUtil {

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
}
