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
package org.smartdata.rule.parser;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ParseTree;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.rule.TranslateResult;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parser a rule string and translate it.
 */
public class RuleStringParser {
  private String rule;
  private TranslationContext ctx = null;

  private static Map<String, String> optCond = new HashMap<>();
  static {
    optCond.put("allssd", "storagePolicy != \"ALL_SSD\"");
    optCond.put("onessd", "storagePolicy != \"ONE_SSD\"");
    optCond.put("archive", "storagePolicy != \"COLD\"");
    optCond.put("cache", "not inCache");
    optCond.put("uncache", "inCache");
  }

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
      parserErrorMessage += "Line " + line + ", Char " + charPositionInLine
          + " : " + msg + "\n";
      parseErrors.add(e);
    }
  }

  public RuleStringParser(String rule, TranslationContext ctx) {
    this.rule = rule;
    this.ctx = ctx;
  }

  public TranslateResult translate() throws IOException {
    TranslateResult tr = doTranslate(rule);
    CmdletDescriptor cmdDes = tr.getCmdDescriptor();
    if (cmdDes.actionSize() == 0) {
      throw new IOException("No cmdlet specified in Rule");
    }
    String actName = cmdDes.getActionName(0);
    if (cmdDes.actionSize() != 1 || optCond.get(actName) == null) {
      return tr;
    }
    int[] condPosition = tr.getCondPosition();
    String cond = rule.substring(condPosition[0], condPosition[1] + 1);
    String optRule = rule.replace(cond,
        optCond.get(actName) + " and (" + cond + ")");
    return doTranslate(optRule);
  }

  private TranslateResult doTranslate(String rule) throws IOException {
    parseErrors.clear();
    parserErrorMessage = "";

    InputStream input = new ByteArrayInputStream(rule.getBytes());
    ANTLRInputStream antlrInput = new ANTLRInputStream(input);
    SmartRuleLexer lexer = new SmartRuleLexer(antlrInput);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SmartRuleParser parser = new SmartRuleParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(new SSMRuleErrorListener());
    ParseTree tree = parser.ssmrule();

    if (parseErrors.size() > 0) {
      throw new IOException(parserErrorMessage);
    }

    SmartRuleVisitTranslator visitor = new SmartRuleVisitTranslator(ctx);
    try {
      visitor.visit(tree);
    } catch (RuntimeException e) {
      throw new IOException(e.getMessage());
    }

    TranslateResult result = visitor.generateSql();
    return result;
  }
}
