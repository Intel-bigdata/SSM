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
package org.smartdata.server.rule.parser;


import org.antlr.v4.runtime.ParserRuleContext;
import org.smartdata.common.actions.ActionType;
import org.smartdata.server.command.CommandDescriptor;
import org.smartdata.server.rule.exceptions.RuleParserException;
import org.smartdata.server.rule.objects.Property;
import org.smartdata.server.rule.objects.PropertyRealParas;
import org.smartdata.server.rule.objects.SmartObject;
import org.smartdata.server.metastore.TableMetaData;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Convert SSM parse tree into internal representation.
 */
public class SmartRuleVisitTranslator extends SmartRuleBaseVisitor<TreeNode> {
  private Map<String, SmartObject> objects = new HashMap<>();
  private TreeNode objFilter = null;
  private TreeNode conditions = null;
  private List<PropertyRealParas> realParases = new LinkedList<>();

  private TimeBasedScheduleInfo timeBasedScheduleInfo = null;
  private CommandDescriptor cmdDescriptor = null;

  Map<String, String> actionParams = new HashMap<>();
  ActionType actionType = null;

  private int nError = 0;

  private TreeNode root ;
  private Stack<TreeNode> nodes = new Stack<>();

  private TranslationContext transCtx = null;

  public SmartRuleVisitTranslator() {
  }

  public SmartRuleVisitTranslator(TranslationContext transCtx) {
    this.transCtx = transCtx;
  }

  @Override
  public TreeNode visitRuleLine(SmartRuleParser.RuleLineContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public TreeNode visitObjTypeOnly(SmartRuleParser.ObjTypeOnlyContext ctx) {
    String objName = ctx.OBJECTTYPE().getText();
    SmartObject obj = SmartObject.getInstance(objName);
    objects.put(objName, obj);
    objects.put("Default", obj);
    return null;
  }

  @Override
  public TreeNode visitObjTypeWith(SmartRuleParser.ObjTypeWithContext ctx) {
    String objName = ctx.OBJECTTYPE().getText();
    SmartObject obj = SmartObject.getInstance(objName);
    objects.put(objName, obj);
    objects.put("Default", obj);
    objFilter = visit(ctx.objfilter());
    return null;
  }

  @Override
  public TreeNode visitConditions(SmartRuleParser.ConditionsContext ctx) {
    // System.out.println("Condition: " + ctx.getText());
    conditions = visit(ctx.boolvalue());
    return conditions;
  }

  @Override
  public TreeNode visitTriTimePoint(SmartRuleParser.TriTimePointContext ctx) {
    timeBasedScheduleInfo = new TimeBasedScheduleInfo();
    TreeNode tr = visit(ctx.timepointexpr());
    try {
      long tm = (Long) (tr.eval().getValue());
      timeBasedScheduleInfo.setStartTime(tm);
      timeBasedScheduleInfo.setEndTime(tm);
      return null;
    } catch (IOException e) {
      throw new RuleParserException("Evaluate 'AT' expression error");
    }
  }

  @Override public TreeNode visitTriCycle(SmartRuleParser.TriCycleContext ctx) {
    timeBasedScheduleInfo = new TimeBasedScheduleInfo();
    TreeNode tr = visit(ctx.timeintvalexpr());
    timeBasedScheduleInfo.setEvery(getLongConstFromTreeNode(tr));
    if (ctx.duringexpr() != null) {
      visit(ctx.duringexpr());
    } else {
      timeBasedScheduleInfo.setStartTime(getTimeNow());
      timeBasedScheduleInfo.setEndTime(TimeBasedScheduleInfo.FOR_EVER);
    }
    return null;
  }

  @Override public TreeNode visitTriFileEvent(SmartRuleParser.TriFileEventContext ctx) {
    return visitChildren(ctx);
  }

  // duringexpr : FROM timepointexpr (TO timepointexpr)? ;
  @Override public TreeNode visitDuringexpr(SmartRuleParser.DuringexprContext ctx) {
    TreeNode trFrom = visit(ctx.timepointexpr(0));
    timeBasedScheduleInfo.setStartTime(getLongConstFromTreeNode(trFrom));
    if (ctx.timepointexpr().size() > 1) {
      TreeNode trEnd = visit(ctx.timepointexpr(1));
      timeBasedScheduleInfo.setEndTime(getLongConstFromTreeNode(trEnd));
    } else {
      timeBasedScheduleInfo.setEndTime(TimeBasedScheduleInfo.FOR_EVER);
    }
    return null;
  }

  private long getLongConstFromTreeNode(TreeNode tr) {
    if (tr.isOperNode()) {
      throw new RuleParserException("Should be a ValueNode");
    }

    try {
      return (Long) (tr.eval().getValue());
    } catch (IOException e) {
      throw new RuleParserException("Evaluate ValueNode error:" + tr);
    }
  }

  // time point

  @Override
  public TreeNode visitTpeCurves(SmartRuleParser.TpeCurvesContext ctx) {
    return visit(ctx.getChild(1));
  }

  @Override
  public TreeNode visitTpeNow(SmartRuleParser.TpeNowContext ctx) {
    return new ValueNode(new VisitResult(ValueType.TIMEPOINT, getTimeNow()));
  }

  private long getTimeNow() {
    if (transCtx != null) {
      return transCtx.getSubmitTime();
    }
    return System.currentTimeMillis();
  }

  @Override
  public TreeNode visitTpeTimeConst(SmartRuleParser.TpeTimeConstContext ctx) {
    String text = ctx.getText();
    String tc = text.substring(1, text.length() - 1);
    return pharseConstTimePoint(tc);
  }

  // | timepointexpr ('+' | '-') timeintvalexpr              #tpeTimeExpr
  @Override
  public TreeNode visitTpeTimeExpr(SmartRuleParser.TpeTimeExprContext ctx) {
    return generalExprOpExpr(ctx);
    //return evalLongExpr(ctx, ValueType.TIMEPOINT);
  }

  @Override
  public TreeNode visitTpeTimeId(SmartRuleParser.TpeTimeIdContext ctx) {
    TreeNode node = visitChildren(ctx);
    if (!node.isOperNode()) {
      if (((ValueNode)node).getValueType() == ValueType.TIMEPOINT) {
        return node;
      }
    }
    throw new RuleParserException("Invalid attribute type in expression for '"
        + ctx.getText() + "'");
  }


  // Time interval

  @Override
  public TreeNode visitTieCurves(SmartRuleParser.TieCurvesContext ctx) {
    return visit(ctx.getChild(1));
  }

  // | timepointexpr '-' timepointexpr                       #tieTpExpr
  @Override
  public TreeNode visitTieTpExpr(SmartRuleParser.TieTpExprContext ctx) {
    return generalExprOpExpr(ctx);
    //return evalLongExpr(ctx, ValueType.TIMEINTVAL);
  }

  @Override
  public TreeNode visitTieConst(SmartRuleParser.TieConstContext ctx) {
    return pharseConstTimeInterval(ctx.getText());
  }

  // timeintvalexpr ('-' | '+') timeintvalexpr             #tieTiExpr
  @Override
  public TreeNode visitTieTiExpr(SmartRuleParser.TieTiExprContext ctx) {
    return generalExprOpExpr(ctx);
    //return evalLongExpr(ctx, ValueType.TIMEINTVAL);
  }

  @Override
  public TreeNode visitTieTiIdExpr(SmartRuleParser.TieTiIdExprContext ctx) {
    TreeNode node = visitChildren(ctx);
    if (!node.isOperNode()) {
      if (((ValueNode)node).getValueType() == ValueType.TIMEINTVAL) {
        return node;
      }
    }
    throw new RuleParserException("Invalid attribute type in expression for '"
        + ctx.getText() + "'");
  }


  private SmartObject createIfNotExist(String objName) {
    SmartObject obj = objects.get(objName);
    if (obj == null) {
      obj = SmartObject.getInstance(objName);
      objects.put(objName, obj);
    }
    return obj;
  }

  // ID

  @Override
  public TreeNode visitIdAtt(SmartRuleParser.IdAttContext ctx) {
    // System.out.println("Bare ID: " + ctx.getText());
    Property p = objects.get("Default").getProperty(ctx.getText());
    if (p == null) {
      throw new RuleParserException("Object " + objects.get("Default").toString()
          + " does not have a attribute named '" + "'" + ctx.getText());
    }

    if (p.getParamsTypes() != null) {
      throw new RuleParserException("Should have no parameter(s) for "
          + ctx.getText());
    }
    PropertyRealParas realParas = new PropertyRealParas(p, null);
    realParases.add(realParas);
    return new ValueNode(new VisitResult(p.getValueType(), null, realParas));
  }

  @Override
  public TreeNode visitIdObjAtt(SmartRuleParser.IdObjAttContext ctx) {
    SmartObject obj = createIfNotExist(ctx.OBJECTTYPE().toString());
    Property p = obj.getProperty(ctx.ID().getText());
    if (p == null) {
      throw new RuleParserException("Object " + obj.toString()
          + " does not have a attribute named '" + "'" + ctx.ID().getText());
    }
    if (p.getParamsTypes() != null) {
      throw new RuleParserException("Should have no parameter(s) for "
          + ctx.getText());
    }
    PropertyRealParas realParas = new PropertyRealParas(p, null);
    realParases.add(realParas);
    return new ValueNode(new VisitResult(p.getValueType(), null, realParas));
  }

  @Override
  public TreeNode visitIdAttPara(SmartRuleParser.IdAttParaContext ctx) {
    SmartObject obj = createIfNotExist("Default");
    Property p = obj.getProperty(ctx.ID().getText());
    if (p == null) {
      throw new RuleParserException("Object " + obj.toString()
          + " does not have a attribute named '" + ctx.ID().getText() + "'");
    }

    if (p.getParamsTypes() == null) {
      throw new RuleParserException(obj.toString() + "." + ctx.ID().getText()
          + " does not need parameter(s)");
    }

    int numParameters = ctx.getChildCount() / 2 - 1;
    if (p.getParamsTypes().size() != numParameters) {
      throw new RuleParserException(obj.toString() + "." + ctx.ID().getText()
          + " needs " + p.getParamsTypes().size() + " instead of "
          + numParameters);
    }

    return parseIdParams(ctx, p, 2);
  }

  @Override
  public TreeNode visitIdObjAttPara(SmartRuleParser.IdObjAttParaContext ctx) {
    String objName = ctx.OBJECTTYPE().getText();
    String propertyName = ctx.ID().getText();
    Property p = createIfNotExist(objName).getProperty(propertyName);

    if (p == null) {
      throw new RuleParserException("Object " + ctx.OBJECTTYPE().toString()
          + " does not have a attribute named '" + "'" + ctx.ID().getText());
    }

    if (p.getParamsTypes() == null) {
      throw new RuleParserException(ctx.OBJECTTYPE().toString() + "." + ctx.ID().getText()
          + " does not need parameter(s)");
    }

    int numParameters = ctx.getChildCount() / 2 - 2;
    if (p.getParamsTypes().size() != numParameters) {
      throw new RuleParserException(ctx.OBJECTTYPE().toString() + "." + ctx.ID().getText()
          + " needs " + p.getParamsTypes().size() + " instead of "
          + numParameters);
    }

    return parseIdParams(ctx, p, 4);
  }

  private TreeNode parseIdParams(ParserRuleContext ctx, Property p, int start) {
    int paraIndex = 0;
    List<Object> paras = new ArrayList<>();
    //String a = ctx.getText();
    for (int i = start; i < ctx.getChildCount() - 1; i += 2) {
      String c = ctx.getChild(i).getText();
      TreeNode res = visit(ctx.getChild(i));
      if (res.isOperNode()) {
        throw new RuleParserException("Should be direct.");
      }
      if (res.getValueType() != p.getParamsTypes().get(paraIndex)) {
        throw new RuleParserException("Unexpected parameter type: "
            + ctx.getChild(i).getText());
      }
      paras.add(((ValueNode) res).eval().getValue());
      paraIndex++;
    }
    PropertyRealParas realParas = new PropertyRealParas(p, paras);
    realParases.add(realParas);
    return new ValueNode(new VisitResult(p.getValueType(), null, realParas));
  }

  // numricexpr

  @Override
  public TreeNode visitNumricexprId(SmartRuleParser.NumricexprIdContext ctx) {
    return visit(ctx.id());
  }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override
  public TreeNode visitNumricexprCurve(SmartRuleParser.NumricexprCurveContext ctx) {
    return visit(ctx.numricexpr());
  }

  // numricexpr opr numricexpr
  @Override
  public TreeNode visitNumricexprAdd(SmartRuleParser.NumricexprAddContext ctx) {
    return generalExprOpExpr(ctx);
  }

  @Override
  public TreeNode visitNumricexprMul(SmartRuleParser.NumricexprMulContext ctx) {
    return generalExprOpExpr(ctx);
  }

  @Override
  public TreeNode visitNumricexprLong(SmartRuleParser.NumricexprLongContext ctx) {
    return pharseConstLong(ctx.LONG().getText());
  }

  private TreeNode generalExprOpExpr(ParserRuleContext ctx) {
    TreeNode r1 = visit(ctx.getChild(0));
    TreeNode r2 = visit(ctx.getChild(2));
    return generalHandleExpr(ctx.getChild(1).getText(), r1, r2);
  }

  private TreeNode generalHandleExpr(String operator, TreeNode left, TreeNode right) {
    TreeNode ret;
    try {
      if (left.isOperNode() ||  right.isOperNode()) {
        ret = new OperNode(OperatorType.fromString(operator), left, right);
      } else if (left.eval().isConst() && right.eval().isConst()) {
        ret = new ValueNode(left.eval().eval(OperatorType.fromString(operator), right.eval()));
      } else {
        ret = new OperNode(OperatorType.fromString(operator), left, right);
      }
    } catch (IOException e) {
      throw new RuleParserException(e.getMessage());
    }
    return ret;
  }

  // bool value
  @Override
  public TreeNode visitBvAndOR(SmartRuleParser.BvAndORContext ctx) {
    return generalExprOpExpr(ctx);
  }

  @Override
  public TreeNode visitBvId(SmartRuleParser.BvIdContext ctx) {
    return visit(ctx.id());
  }

  @Override
  public TreeNode visitBvNot(SmartRuleParser.BvNotContext ctx) {
    TreeNode left = visit(ctx.boolvalue());
    // TODO: bypass null
    TreeNode right = new ValueNode(new VisitResult(ValueType.BOOLEAN, null));
    return generalHandleExpr(ctx.NOT().getText(), left, right);
  }

  @Override
  public TreeNode visitBvCurve(SmartRuleParser.BvCurveContext ctx) {
    return visit(ctx.boolvalue());
  }

  @Override
  public TreeNode visitBvCompareexpr(SmartRuleParser.BvCompareexprContext ctx) {
    return visit(ctx.compareexpr());
  }

  // Compare

  @Override
  public TreeNode visitCmpIdLong(SmartRuleParser.CmpIdLongContext ctx) {
    return generalExprOpExpr(ctx);
  }

  @Override
  public TreeNode visitCmpIdString(SmartRuleParser.CmpIdStringContext ctx) {
    return generalExprOpExpr(ctx);
  }

  @Override
  public TreeNode visitCmpIdStringMatches(SmartRuleParser.CmpIdStringMatchesContext ctx) {
    return generalExprOpExpr(ctx);
  }

  @Override
  public TreeNode visitCmpTimeintvalTimeintval(SmartRuleParser.CmpTimeintvalTimeintvalContext ctx) {
    return generalExprOpExpr(ctx);
  }

  @Override
  public TreeNode visitCmpTimepointTimePoint(SmartRuleParser.CmpTimepointTimePointContext ctx) {
    return generalExprOpExpr(ctx);
  }

  // String
  @Override
  public TreeNode visitStrPlus(SmartRuleParser.StrPlusContext ctx) {
    return generalExprOpExpr(ctx);
  }


  @Override
  public TreeNode visitStrOrdString(SmartRuleParser.StrOrdStringContext ctx) {
    return pharseConstString(ctx.STRING().getText());
  }

  @Override
  public TreeNode visitStrID(SmartRuleParser.StrIDContext ctx) {
    return visit(ctx.id());
  }

  @Override
  public TreeNode visitStrCurve(SmartRuleParser.StrCurveContext ctx) {
    return visit(ctx.getChild(1));
  }

  @Override
  public TreeNode visitStrTimePointStr(SmartRuleParser.StrTimePointStrContext ctx) {
    return new ValueNode(new VisitResult(ValueType.STRING,
        ctx.TIMEPOINTCONST().getText()));
  }

  @Override
  public TreeNode visitConstLong(SmartRuleParser.ConstLongContext ctx) {
    return pharseConstLong(ctx.getText());
  }

  @Override
  public TreeNode visitConstString(SmartRuleParser.ConstStringContext ctx) {
    return pharseConstString(ctx.getText());
  }

  @Override
  public TreeNode visitConstTimeInverval(SmartRuleParser.ConstTimeInvervalContext ctx) {
    return pharseConstTimeInterval(ctx.getText());
  }

  @Override
  public TreeNode visitConstTimePoint(SmartRuleParser.ConstTimePointContext ctx) {
    return pharseConstTimePoint(ctx.getText());
  }

  private TreeNode pharseConstTimeInterval(String str) {
    long intval = 0L;
    Pattern p = Pattern.compile("([0-9]+)([a-z]+)");
    Matcher m = p.matcher(str);
    int start = 0;
    while (m.find(start)) {
      String digStr = m.group(1);
      String unitStr = m.group(2);
      long value = 0;
      try {
        value = Long.parseLong(digStr);
      } catch (NumberFormatException e) {
      }

      switch (unitStr) {
        case "d":
        case "day":
          intval += value * 24 * 3600 * 1000;
          break;
        case "h":
        case "hour":
          intval += value * 3600 * 1000;
          break;
        case "m":
        case "min":
          intval += value * 60 * 1000;
          break;
        case "s":
        case "sec":
          intval += value * 1000;
          break;
      }
      start += m.group().length();
    }
    return new ValueNode(new VisitResult(ValueType.TIMEINTVAL, intval));
  }

  private TreeNode pharseConstString(String str) {
    String ret = str.substring(1, str.length() - 1);
    return new ValueNode(new VisitResult(ValueType.STRING, ret));
  }

  private TreeNode pharseConstLong(String strLong) {
    String str = strLong.toUpperCase();
    Long ret = 0L;
    long times = 1;
    try {
      Pattern p = Pattern.compile("[PTGMK]?B");
      Matcher m = p.matcher(str);
      String unit = "";
      if (m.find()) {
        unit = m.group();
      }
      str = str.substring(0, str.length() - unit.length());
      switch (unit) {
        case "PB":
          times *= 1024;
        case "TB":
          times *= 1024;
        case "GB":
          times *= 1024;
        case "MB":
          times *= 1024;
        case "KB":
          times *= 1024;
      }
      ret = Long.parseLong(str);
    } catch (NumberFormatException e) {
      // ignore, impossible
    }
    return new ValueNode(new VisitResult(ValueType.LONG, ret * times));
  }

  @Override
  public TreeNode visitCommand(SmartRuleParser.CommandContext ctx) {
    String cmd = ctx.getText();
    try {
      cmdDescriptor = CommandDescriptor.fromCommandString(cmd);
    } catch (ParseException e) {
      throw new RuleParserException(e.getMessage());
    }
    return null;
  }

  public TreeNode pharseConstTimePoint(String str) {
    SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    TreeNode result;
    Date date;
    try {
      date = ft.parse(str);
      result = new ValueNode(
          new VisitResult(ValueType.TIMEPOINT, date.getTime()));
    } catch (ParseException e) {
      result = new ValueNode(new VisitResult());
    }
    return result;
  }

  private void setDefaultTimeBasedScheduleInfo() {
    // TODO: using a more sophisticated way
    if (timeBasedScheduleInfo == null) {
      timeBasedScheduleInfo = new TimeBasedScheduleInfo(getTimeNow(),
          TimeBasedScheduleInfo.FOR_EVER, 5000);
    }
  }

  List<String> sqlStatements = new LinkedList<>();
  List<String> tempTableNames = new LinkedList<>();
  Map<String, List<Object>> dynamicParameters = new HashMap<>();

  public TranslateResult generateSql() throws IOException {
    String ret = "";
    TreeNode l = objFilter != null ? objFilter : conditions;
    TreeNode r = objFilter == null ? objFilter : conditions;
    switch (objects.get("Default").getType()) {
      case DIRECTORY:
      case FILE:
        ret = "SELECT path FROM files";
        break;
      default:
        throw new IOException("No operation defined for Object "
            + objects.get("Default").getType());
    }

    if (l != null) {
      TreeNode actRoot = r == null ? l : new OperNode(OperatorType.AND, l, r);
      TreeNode root = new OperNode(OperatorType.NONE, actRoot, null);
      // TODO: only file now
      ret += " WHERE " + doGenerateSql(root, "files").getRet() + ";";
    }

    sqlStatements.add(ret);
    setDefaultTimeBasedScheduleInfo();

    return new TranslateResult(sqlStatements,
        tempTableNames, dynamicParameters, sqlStatements.size() - 1,
        timeBasedScheduleInfo, cmdDescriptor);
  }

  private class NodeTransResult {
    private String tableName;
    private String ret;

    public NodeTransResult(String tableName, String ret) {
      this.tableName = tableName;
      this.ret = ret;
    }

    public String getTableName() {
      return tableName;
    }

    public String getRet() {
      return ret;
    }
  }

  private String connectTables(String baseTable, NodeTransResult curr) {
    String[] key =
        TableMetaData.getJoinableKey(baseTable, curr.getTableName());
    String subSql = null;
    if (key == null) {
      return "(SELECT COUNT(*) FROM " + curr.getTableName()
          + (curr.getRet() != null ? " WHERE (" + curr.getRet() + ")" : "")
          + ") <> 0";
    } else {
      return key[0] + " IN "
          + "(SELECT " + key[1] + " FROM " + curr.getTableName()
          + (curr.getRet() != null ? " WHERE (" + curr.getRet() + ")" : "")
          + ")";
    }
  }

  public NodeTransResult doGenerateSql(TreeNode root, String tableName)
      throws IOException {
    if (root == null) {
      return new NodeTransResult(tableName, "");
    }

    if (root.isOperNode()) {
      OperatorType optype = ((OperNode) root).getOperatorType();
      String op = optype.getOpInSql();
      NodeTransResult lop = doGenerateSql(root.getLeft(), tableName);
      NodeTransResult rop = null;
      if (optype != OperatorType.NOT) {
        rop = doGenerateSql(root.getRight(), tableName);
      }

      if (lop.getTableName() == null && rop.getTableName() != null) {
        NodeTransResult temp = lop;
        lop = rop;
        rop = temp;
      }

      if (optype == OperatorType.AND || optype == OperatorType.OR
          || optype == OperatorType.NONE) {
        String lopTable = lop.getTableName();
        if (lopTable != null && !lopTable.equals(tableName)) {
          lop = new NodeTransResult(tableName, connectTables(tableName, lop));
        }

        String ropTable = rop.getTableName();
        if (ropTable != null && !ropTable.equals(tableName)) {
          rop = new NodeTransResult(tableName, connectTables(tableName, rop));
        }
      }

      if (optype == OperatorType.NOT) {
        return new NodeTransResult(tableName,
            op + " " + connectTables(tableName, lop));
      }

      String res;
      if (op.length() > 0) {
        res = "(" + lop.getRet() + " " + op + " " + rop.getRet() + ")";
      } else {
        res = "(" + lop.getRet() + ")";
      }

      return new NodeTransResult(
          lop.getTableName() != null ? lop.getTableName() : rop.getTableName(),
          res);

    } else {
      ValueNode vNode = (ValueNode) root;
      VisitResult vr = vNode.eval();
      if (vr.isConst()) {
        switch (vr.getValueType()) {
          case TIMEINTVAL:
          case TIMEPOINT:
          case LONG:
            return new NodeTransResult(null, "" + ((Long) vr.getValue()));
          case STRING:
            return new NodeTransResult(null,
                "'" + ((String) vr.getValue()) + "'");
          case BOOLEAN:
            if ((Boolean)vr.getValue()) {
              return new NodeTransResult(null, "1");
            } else {
              return new NodeTransResult(null, "0");
            }
          // TODO: for other types
          default:
            throw new IOException("Type = " + vr.getValueType().toString());
        }
      } else {
        PropertyRealParas realParas = vr.getRealParas();
        Property p = realParas.getProperty();
        // TODO: hard code now, abstract later
        if (p.getPropertyName() == "accessCount") {
          String rid = "";
          if (transCtx != null) {
            rid = transCtx.getRuleId() + "_";
          }
          String virTab = "VIR_ACC_CNT_TAB_" + rid + realParas.instId();
          if (!tempTableNames.contains(virTab)) {
            tempTableNames.add(virTab);
            sqlStatements.add("DROP TABLE IF EXISTS '" + virTab + "';");
            sqlStatements.add("$@genVirtualAccessCountTable(" + virTab + ")");
            dynamicParameters.put(virTab,
                Arrays.asList(realParas.getValues(), virTab));
          }
          return new NodeTransResult(virTab,
              realParas.formatParameters());
        }

        return new NodeTransResult(p.getTableName(),
            realParas.formatParameters());
      }
    }
    //return new NodeTransResult(tableName, "");
  }
}
