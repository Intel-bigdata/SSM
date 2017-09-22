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

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.rule.parser.SmartRuleLexer;
import org.smartdata.rule.parser.SmartRuleParser;
import org.smartdata.rule.parser.SmartRuleVisitTranslator;
import org.smartdata.model.rule.TranslateResult;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class TestSmartRuleParser {
  List<RecognitionException> parseErrors = new ArrayList<RecognitionException>();

  public class SSMRuleErrorListener extends BaseErrorListener {
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer,
        Object offendingSymbol, int line, int charPositionInLine,
        String msg, RecognitionException e) {
      List<String> stack = ((Parser)recognizer).getRuleInvocationStack();
      Collections.reverse(stack);
      System.err.println("rule stack: " + stack);
      System.err.println("line " + line + ":" + charPositionInLine + " at "
          + offendingSymbol + ": " + msg);
      parseErrors.add(e);
    }
  }

  @Test
  public void testValidRule() throws Exception {
    List<String> rules = new LinkedList<>();
    rules.add("file : accessCount(10m) > 10 and accessCount(10m) < 20 "
        + "| cache");
    rules.add("file with path matches \"/a/b*.dat\"  : "
        + "every 5s from \"2013-07-09 19:21:34\" to now + (7d + 4s ) | "
        + "inCache or accessCount(10m) > 10 and 10d > 20s | cache");
    rules.add("file with length > 1GB :  "
        + "blocksize > 1 + 3 and accessCount(30s) > 3 "
        + "and storage.free(\"SSD\") > 100 | cache");
    rules.add("file with length > 3 : "
        + "storage.free(\"SSD\") > 100 and not inCache | cache");
    rules.add("file : accessCount(10min) > 20 | cache");
    rules.add("file: every 5s from now to now + 10d | length > 3 | cache");
    rules.add("file: every 5s | length > 100mb | onessd");
    rules.add("file : every 1s | age > 100day | cache");
    rules.add("file : every 1s | mtime > \"2016-09-13 12:05:06\" | cache");
    rules.add("file : every 1s | mtime > now - 70day | cache");
    rules.add("file : every 1s | storagePolicy == \"ALL_SSD\" | cache");
    rules.add("file : accessCount(10min) < 20 | uncache");
    rules.add("file : accessCount(10min) == 0 | uncache");
    rules.add("file : accessCount(10min) <= 1 | uncache");
    rules.add("file : accessCount(1min) > 5 | cache -replica 2");
    rules.add("file : age <= 1 | hello -print_message \"crul world\"");

    for (String rule : rules) {
      parseAndExecuteRule(rule);
    }
  }

  @Test
  public void testInvalidRule() throws Exception {
    List<String> rules = new LinkedList<>();
    rules.add("someobject: length > 3mb | cache");
    rules.add("file : length > 3day | cache");
    rules.add("file : length() > 3tb | cache");
    rules.add("file : accessCount(10m) > 2 and length() > 3 | cache");
    rules.add("file : every 1s | mtime > 100s | cache");

    for (String rule : rules) {
      try {
        parseAndExecuteRule(rule);
        Assert.fail("Should have exception here!");
      } catch (Exception e) {
        // ignore
      }
    }
  }

  private void parseAndExecuteRule(String rule) throws Exception {
    System.out.println("--> " + rule);
    InputStream input = new ByteArrayInputStream(rule.getBytes());
    ANTLRInputStream antlrInput = new ANTLRInputStream(input);
    SmartRuleLexer lexer = new SmartRuleLexer(antlrInput);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SmartRuleParser parser = new SmartRuleParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(new SSMRuleErrorListener());
    ParseTree tree = parser.ssmrule();
    System.out.println("Parser tree: " + tree.toStringTree(parser));
    System.out.println("Total number of errors: " + parseErrors.size());

    SmartRuleVisitTranslator visitor = new SmartRuleVisitTranslator();
    visitor.visit(tree);

    System.out.println("\nQuery:");
    TranslateResult result = visitor.generateSql();
    int index = 1;
    for (String sql : result.getSqlStatements()) {
      System.out.println("" + index + ". " + sql);
      index++;
    }

    if (parseErrors.size() > 0) {
      throw new IOException("Error while parse rule");
    }
  }
}
