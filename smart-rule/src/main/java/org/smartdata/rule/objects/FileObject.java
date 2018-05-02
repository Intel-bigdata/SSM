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
package org.smartdata.rule.objects;


import org.smartdata.rule.parser.ValueType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Definition of rule object 'File'.
 */
public class FileObject extends SmartObject {

  public static final Map<String, Property> PROPERTIES;

  static {
    PROPERTIES = new HashMap<>();
    PROPERTIES.put("path",
        new Property("path", ValueType.STRING, null, "file", "path", false));
    PROPERTIES.put("accessCount",
        new Property("accessCount", ValueType.LONG,
            Arrays.asList(ValueType.TIMEINTVAL),
            "VIRTUAL_ACCESS_COUNT_TABLE", "", false, "count"));
    PROPERTIES.put("accessCountMax",
        new Property("accessCountMax", ValueType.LONG,
            Arrays.asList(ValueType.TIMEINTVAL, ValueType.LONG),
            "VIRTUAL_ACCESS_COUNT_TABLE", "", false, "count"));
    PROPERTIES.put("length",
        new Property("length", ValueType.LONG,
            null, "file", "length", false));
    PROPERTIES.put("blocksize",
        new Property("blocksize", ValueType.LONG,
            null, "file", "block_size", false));
    PROPERTIES.put("inCache",
        new Property("inCache", ValueType.BOOLEAN,
            null, "cached_file", null, false));
    PROPERTIES.put("age",
        new Property("age", ValueType.TIMEINTVAL,
            null, "file", null, false,
            "($NOW - modification_time)"));
    PROPERTIES.put("mtime",
        new Property("mtime", ValueType.TIMEPOINT,
            null, "file", "modification_time", false));
    PROPERTIES.put("atime",
        new Property("atime", ValueType.TIMEPOINT,
            null, "file", "access_time", false));
    PROPERTIES.put("storagePolicy",
        new Property("storagePolicy", ValueType.STRING,
            null, "file", null, false,
            "(SELECT policy_name FROM storage_policy WHERE sid = file.sid)"));
    PROPERTIES.put("unsynced",
        new Property("unsynced", ValueType.BOOLEAN,
            null, "file_diff", null, false,
            "state = 0"));
    PROPERTIES.put("isDir",
        new Property("isDir", ValueType.BOOLEAN,
            null, "file", "is_dir", false));
  }

  public FileObject() {
    super(ObjectType.FILE);
  }

  public Map<String, Property> getProperties() {
    return PROPERTIES;
  }
}
