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
package org.smartdata.server.utils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.List;
import java.util.Map;

/**
 * Json utilities for REST APIs.
 */
public class JsonUtil {

  public static String toJsonString(Map<String, String> map) {
    Gson gson = new Gson();
    return gson.toJson(map);
  }

  public static String toJsonString(List<Map<String, String>> listMap) {
    Gson gson = new Gson();
    return gson.toJson(listMap, new TypeToken<List<Map<String, String>>>(){}.getType());
  }

  public static List<Map<String, String>> toArrayListMap(String jsonString) {
    Gson gson = new Gson();
    List<Map<String, String>> listMap = gson.fromJson(jsonString,
            new TypeToken<List<Map<String, String>>>(){}.getType());
    return listMap;
  }

  public static Map<String, String> toStringStringMap(String jsonString) {
    Gson gson = new Gson();
    Map<String, String> res = gson.fromJson(jsonString,
        new TypeToken<Map<String, String>>(){}.getType());
    return res;
  }
}
