/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.hadoop.ssm.rule.parser.SSMRuleLexer;
import org.apache.hadoop.ssm.rule.parser.SSMRuleParser;
import org.apache.hadoop.ssm.rule.parser.SSMRuleVisitTranslator;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by root on 3/21/17.
 */
public class TestSSMRuleParser {
  List<RecognitionException> parseErrors = new ArrayList<RecognitionException>();

  public class SSMRuleErrorListener extends BaseErrorListener {
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer,
                            Object offendingSymbol,
                            int line, int charPositionInLine,
                            String msg,
                            RecognitionException e)
    {
      List<String> stack = ((Parser)recognizer).getRuleInvocationStack();
      Collections.reverse(stack);
      System.err.println("rule stack: "+stack);
      System.err.println("line "+line+":"+charPositionInLine+" at "+
          offendingSymbol+": "+msg);
    }
  }

  @Test
  public void parseRule() throws IOException {
    String rule = "file with path matches \"/a/b*.dat\"  : "
        + "on FileCreate from \"2013-07-09 19:21:34\" to now + (7d + 4s ) | "
        + "isincache and accessCount(10m) > 10 and x == y and "
        + "x matches \"hello\" and \"/file/*.db\" matches file.path "
        + "and true or c > 10 and 100 > d or 10d > 20s | delete";
    String rule1 = "file with length > 1GB :  and  blocksize > 1 + 3 | delete";
    InputStream input = new ByteArrayInputStream(rule1.getBytes());
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
  }
}
