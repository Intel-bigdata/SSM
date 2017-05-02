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
package org.apache.hadoop.ssm.rule.parser;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ParseTree;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Parser a rule string and translate it.
 * // TODO: Refine exceptions and error handling
 */
public class RuleStringParser {
  private String rule;
  private ParseTree tree = null;

  List<RecognitionException> parseErrors = new ArrayList<RecognitionException>();
  String parserErrorMessage = "";

  public class SSMRuleErrorListener extends BaseErrorListener {
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer,
                            Object offendingSymbol,
                            int line, int charPositionInLine,
                            String msg,
                            RecognitionException e) {
      List<String> stack = ((Parser)recognizer).getRuleInvocationStack();
      Collections.reverse(stack);
      parserErrorMessage += "Line " + line + ":" + charPositionInLine + " : "
          + msg + "\n";
      parseErrors.add(e);
    }
  }

  public RuleStringParser(String rule) {
    this.rule = rule;
  }

  public TranslateResult translate() throws IOException {
    InputStream input = new ByteArrayInputStream(rule.getBytes());
    ANTLRInputStream antlrInput = new ANTLRInputStream(input);
    SSMRuleLexer lexer = new SSMRuleLexer(antlrInput);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SSMRuleParser parser = new SSMRuleParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(new SSMRuleErrorListener());
    tree = parser.ssmrule();

    // TODO: return errors as much as possible

    if (parseErrors.size() > 0) {
      throw new IOException(parserErrorMessage);
    }

    SSMRuleVisitTranslator visitor = new SSMRuleVisitTranslator();
    try {
      visitor.visit(tree);
    } catch (RuntimeException e) {
      throw new IOException(e.getMessage());
    }

    TranslateResult result = visitor.generateSql();
    return result;
  }
}
