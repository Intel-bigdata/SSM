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
package org.apache.hadoop.ssm;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.hadoop.ssm.rule.parser.SSMRuleLexer;
import org.apache.hadoop.ssm.rule.parser.SSMRuleParser;
import org.apache.hadoop.ssm.rule.parser.SSMRuleVisitTranslator;
import org.apache.hadoop.ssm.rule.parser.TranslateResult;
import org.apache.hadoop.ssm.sql.DBAdapter;
import org.apache.hadoop.ssm.sql.ExecutionContext;
import org.apache.hadoop.ssm.rule.RuleQueryExecutor;
import org.apache.hadoop.ssm.sql.TestDBUtil;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestSSMRuleParser {
  List<RecognitionException> parseErrors = new ArrayList<RecognitionException>();

  public class SSMRuleErrorListener extends BaseErrorListener {
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer,
                            Object offendingSymbol,
                            int line, int charPositionInLine,
                            String msg,
                            RecognitionException e) {
      List<String> stack = ((Parser)recognizer).getRuleInvocationStack();
      Collections.reverse(stack);
      System.err.println("rule stack: "+stack);
      System.err.println("line "+line+":"+charPositionInLine+" at "+
          offendingSymbol+": "+msg);
      parseErrors.add(e);
    }
  }

  @Test
  public void parseRule() throws Exception {
    String rule0 = "file with path matches \"/a/b*.dat\"  : "
        + "on FileCreate from \"2013-07-09 19:21:34\" to now + (7d + 4s ) | "
        + "isincache and accessCount(10m) > 10 and x == y and "
        + "x matches \"hello\" and \"/file/*.db\" matches file.path "
        + "and true or c > 10 and 100 > d or 10d > 20s | cachefile";
    String rule1 = "file with length > 1GB :  "
        + "blocksize > 1 + 3 and accessCount(30s) > 3 "
        + "and storage.free(\"SSD\") > 100 | cachefile";
    String rule2 = "file with length > 3 : "
        + "storage.free(\"SSD\") > 100 and not inCache | cachefile";
    String rule3 = "file : accessCount(10m) > 20 | cachefile";
    String rule4 = "file : accessCountX(10m) > 2 and length() > 3 | cachefile";
    String rule5 = "file: every 5s from now to now + 100d | length > 3 | cachefile";
    String rule = rule5;
    InputStream input = new ByteArrayInputStream(rule.getBytes());
    ANTLRInputStream antlrInput = new ANTLRInputStream(input);
    SSMRuleLexer lexer = new SSMRuleLexer(antlrInput);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SSMRuleParser parser = new SSMRuleParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(new SSMRuleErrorListener());
    ParseTree tree = parser.ssmrule();
    System.out.println("Parser tree: " + tree.toStringTree(parser));
    System.out.println("Total number of errors: " + parseErrors.size());

    SSMRuleVisitTranslator visitor = new SSMRuleVisitTranslator();
    visitor.visit(tree);

    System.out.println("\nQuery:");
    TranslateResult result = visitor.generateSql();
    int index = 1;
    for (String sql : result.getSqlStatements()) {
      System.out.println("" + index + ". " + sql);
      index++;
    }

    ExecutionContext ctx = new ExecutionContext();
    DBAdapter dbAdapter = new DBAdapter(TestDBUtil.getTestDBInstance());
    RuleQueryExecutor qe = new RuleQueryExecutor(null, ctx, result, dbAdapter);
    List<String> paths = qe.executeFileRuleQuery();
    index = 1;
    System.out.println("\nFiles:");
    for (String path : paths) {
      System.out.println("" + index + ". " + path);
      index++;
    }
  }
}
