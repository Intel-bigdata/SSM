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


import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.hadoop.ssm.rule.objects.SSMObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Convert SSM parse tree into internal representation.
 */
public class SSMRuleVisitTranslator extends SSMRuleBaseVisitor<VisitResult> {
  private SSMObject targetObject;

  @Override
  public VisitResult visitObjTypeOnly(SSMRuleParser.ObjTypeOnlyContext ctx) {
    System.out.println("XXXXXXXXXXXXXXX");
    targetObject = SSMObject.getInstance(ctx.OBJECTTYPE().getText());
    return visitChildren(ctx);
  }

  @Override
  public VisitResult visitObjTypeWith(SSMRuleParser.ObjTypeWithContext ctx) {
    System.out.println("YYYYYYYYY");
    targetObject = SSMObject.getInstance(ctx.OBJECTTYPE().getText());
    return visitChildren(ctx);
  }

  @Override
  public VisitResult visitTriTimePoint(SSMRuleParser.TriTimePointContext ctx) {
    return visitChildren(ctx);
  }

  // time point

  @Override
  public VisitResult visitTpeCurves(SSMRuleParser.TpeCurvesContext ctx) {
    return visit(ctx.getChild(1));
  }

  @Override
  public VisitResult visitTpeNow(SSMRuleParser.TpeNowContext ctx) {
    return new VisitResult(ValueType.TIMEPOINT, System.currentTimeMillis());
  }

  @Override
  public VisitResult visitTpeTimeConst(SSMRuleParser.TpeTimeConstContext ctx) {
    SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    VisitResult result;
    Date date;
    try {
      date = ft.parse(ctx.getText());
      result = new VisitResult(ValueType.TIMEPOINT, date.getTime());
    } catch (ParseException e) {
      result = new VisitResult();
    }
    return result;
  }

  @Override
  public VisitResult visitTpeTimeExpr(SSMRuleParser.TpeTimeExprContext ctx) {
    return evalLongExpr(ctx, ValueType.TIMEPOINT);
  }


  // Time interval

  @Override
  public VisitResult visitTieCurves(SSMRuleParser.TieCurvesContext ctx) {
    return visit(ctx.getChild(1));
  }

  @Override
  public VisitResult visitTieTpExpr(SSMRuleParser.TieTpExprContext ctx) {
    return evalLongExpr(ctx, ValueType.TIMEINTVAL);
  }

  @Override
  public VisitResult visitTieConst(SSMRuleParser.TieConstContext ctx) {
    long intval = 0L;
    String str = ctx.getText();
    Pattern p = Pattern.compile("[0-9]+");
    Matcher m = p.matcher(str);
    int start = 0;
    while (m.find(start)) {
      String digStr = m.group();
      long value = 0;
      try {
        value = Long.parseLong(digStr);
      } catch (NumberFormatException e) {
      }
      char suffix = str.charAt(start + digStr.length());
      switch (suffix) {
        case 'd':
          intval += value * 24 * 3600 * 1000;
          break;
        case 'h':
          intval += value * 3600 * 1000;
          break;
        case 'm':
          intval += value * 60 * 1000;
          break;
        case 's':
          intval += value * 1000;
          break;
      }
      start += m.group().length() + 1;
    }
    return new VisitResult(ValueType.TIMEINTVAL, intval);
  }

  @Override
  public VisitResult visitTieTiExpr(SSMRuleParser.TieTiExprContext ctx) {
    return evalLongExpr(ctx, ValueType.TIMEINTVAL);
  }

  private VisitResult evalLongExpr(ParserRuleContext ctx, ValueType retType) {
    VisitResult r1 = visit(ctx.getChild(0));
    VisitResult r2 = visit(ctx.getChild(2));
    VisitResult r = new VisitResult(retType);
    String op = ctx.getChild(1).getText();
    switch (op) {
      case "+":
        r.setValue((Long)r1.getValue() + (Long)r2.getValue());
        break;
      case "-":
        r.setValue((Long)r1.getValue() - (Long)r2.getValue());
        break;
      case "*":
        r.setValue((Long)r1.getValue() * (Long)r2.getValue());
        break;
      case "/":
        r.setValue((Long)r1.getValue() / (Long)r2.getValue());
        break;
      case "%":
        r.setValue((Long)r1.getValue() % (Long)r2.getValue());
        break;
      default:
        System.out.println("Error: " + ctx.getText());
        r = new VisitResult();
        break;
    }
    return r;
  }
}
