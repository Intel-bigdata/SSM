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
package org.smartdata.rule;

import org.junit.Test;
import org.smartdata.model.rule.TranslateResult;
import org.smartdata.rule.parser.SmartRuleStringParser;
import org.smartdata.rule.parser.TranslationContext;

import java.util.LinkedList;
import java.util.List;

public class TestSmartRuleStringParser {

  @Test
  public void testRuleTranslate() throws Exception {
    List<String> rules = new LinkedList<>();
    rules.add("file : path matches \"/test/*\" | sync -dest \"hdfs://remotecluster:port/somedir\"");
    for (String rule : rules) {
      parseRule(rule);
    }
  }

  private void parseRule(String rule) throws Exception {
    TranslationContext tc = new TranslationContext(1, System.currentTimeMillis());
    SmartRuleStringParser parser = new SmartRuleStringParser(rule, tc);
    TranslateResult tr = parser.translate();

    int index = 1;
    for (String sql : tr.getSqlStatements()) {
      System.out.println("" + index + ". " + sql);
      index++;
    }
  }
}
