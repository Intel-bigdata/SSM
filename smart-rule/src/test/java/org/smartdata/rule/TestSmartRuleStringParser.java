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
    rules.add("file : path matches \"/src/*\" | sync -dest \"hdfs://remotecluster:port/dest\"");
    rules.add("file : accessCount(10min) > accessCountTop(10min, 10) | sleep -ms 0");
    rules.add("file : ac(10min) > acTop(10min, 10) | sleep -ms 0");
    rules.add("file : accessCount(10min) > accessCountBottom(10min, 10) | sleep -ms 0");
    rules.add("file : ac(10min) > acBot(10min, 10) | sleep -ms 0");
    rules.add("file : ac(10min) > accessCountTopOnStoragePolicy(10min, 10, \"ALL_SSD\") "
        + "| sleep -ms 0");
    rules.add("file : ac(10min) > acTopSp(10min, 10, \"ALL_SSD\") | sleep -ms 0");
    rules.add("file : ac(10min) > accessCountBottomOnStoragePolicy(10min, 10, \"CACHE\") "
        + "| sleep -ms 0");
    rules.add("file : ac(10min) > acBotSp(10min, 10, \"CACHE\") | sleep -ms 0");
    rules.add("file : ac(10min) > acBotSp(10min, 10, \"HOT\") and acBotSp(10min, 10, \"HOT\") > 0 "
        + "| sleep -ms 0");
    rules.add("file : every 5h / 1h/ 20min | length > 19 | sleep -ms 10");

    for (String rule : rules) {
      parseRule(rule);
    }
  }

  private void parseRule(String rule) throws Exception {
    TranslationContext tc = new TranslationContext(1, System.currentTimeMillis());
    SmartRuleStringParser parser = new SmartRuleStringParser(rule, tc);
    TranslateResult tr = parser.translate();

    int index = 1;
    System.out.println("\n" + rule);
    for (String sql : tr.getSqlStatements()) {
      System.out.println("\t" + index + ". " + sql);
      index++;
    }
  }
}
