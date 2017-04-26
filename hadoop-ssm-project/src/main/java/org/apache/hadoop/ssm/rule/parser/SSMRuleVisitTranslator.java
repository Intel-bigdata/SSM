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
import org.apache.hadoop.ssm.rule.excepts.RuleParserException;
import org.apache.hadoop.ssm.rule.objects.ObjectType;
import org.apache.hadoop.ssm.rule.objects.Property;
import org.apache.hadoop.ssm.rule.objects.PropertyRealParas;
import org.apache.hadoop.ssm.rule.objects.SSMObject;
import org.apache.hadoop.ssm.sql.TableMetaData;

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
public class SSMRuleVisitTranslator extends SSMRuleBaseVisitor<TreeNode> {
  private Map<String, SSMObject> objects = new HashMap<>();
  private TreeNode objFilter = null;
  private TreeNode conditions = null;
  private List<PropertyRealParas> realParases = new LinkedList<>();

  private int nError = 0;

  private TreeNode root ;
  private Stack<TreeNode> nodes = new Stack<>();

  @Override
  public TreeNode visitRuleLine(SSMRuleParser.RuleLineContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public TreeNode visitObjTypeOnly(SSMRuleParser.ObjTypeOnlyContext ctx) {
    String objName = ctx.OBJECTTYPE().getText();
    SSMObject obj = SSMObject.getInstance(objName);
    objects.put(objName, obj);
    objects.put("Default", obj);
    return null;
  }

  @Override
  public TreeNode visitObjTypeWith(SSMRuleParser.ObjTypeWithContext ctx) {
    String objName = ctx.OBJECTTYPE().getText();
    SSMObject obj = SSMObject.getInstance(objName);
    objects.put(objName, obj);
    objects.put("Default", obj);
    objFilter = visit(ctx.objfilter());
    return null;
  }

  @Override
  public TreeNode visitConditions(SSMRuleParser.ConditionsContext ctx) {
    System.out.println("Condition: " + ctx.getText());
    conditions = visit(ctx.boolvalue());
    return conditions;
  }

  @Override
  public TreeNode visitTriTimePoint(SSMRuleParser.TriTimePointContext ctx) {
    return visitChildren(ctx);
  }

  // time point

  @Override
  public TreeNode visitTpeCurves(SSMRuleParser.TpeCurvesContext ctx) {
    return visit(ctx.getChild(1));
  }

  @Override
  public TreeNode visitTpeNow(SSMRuleParser.TpeNowContext ctx) {
    return new ValueNode(new VisitResult(ValueType.TIMEPOINT, System.currentTimeMillis()));
  }

  @Override
  public TreeNode visitTpeTimeConst(SSMRuleParser.TpeTimeConstContext ctx) {
    return pharseConstTimePoint(ctx.getText());
  }

  // | timepointexpr ('+' | '-') timeintvalexpr              #tpeTimeExpr
  @Override
  public TreeNode visitTpeTimeExpr(SSMRuleParser.TpeTimeExprContext ctx) {
    return generalExprOpExpr(ctx);
    //return evalLongExpr(ctx, ValueType.TIMEPOINT);
  }


  // Time interval

  @Override
  public TreeNode visitTieCurves(SSMRuleParser.TieCurvesContext ctx) {
    return visit(ctx.getChild(1));
  }

  // | timepointexpr '-' timepointexpr                       #tieTpExpr
  @Override
  public TreeNode visitTieTpExpr(SSMRuleParser.TieTpExprContext ctx) {
    return generalExprOpExpr(ctx);
    //return evalLongExpr(ctx, ValueType.TIMEINTVAL);
  }

  @Override
  public TreeNode visitTieConst(SSMRuleParser.TieConstContext ctx) {
    return pharseConstTimeInterval(ctx.getText());
  }

  // timeintvalexpr ('-' | '+') timeintvalexpr             #tieTiExpr
  @Override
  public TreeNode visitTieTiExpr(SSMRuleParser.TieTiExprContext ctx) {
    return generalExprOpExpr(ctx);
    //return evalLongExpr(ctx, ValueType.TIMEINTVAL);
  }


  private SSMObject createIfNotExist(String objName) {
    SSMObject obj = objects.get(objName);
    if (obj == null) {
      obj = SSMObject.getInstance(objName);
      objects.put(objName, obj);
    }
    return obj;
  }

  // ID

  @Override
  public TreeNode visitIdAtt(SSMRuleParser.IdAttContext ctx) {
    System.out.println("Bare ID: " + ctx.getText());
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
  public TreeNode visitIdObjAtt(SSMRuleParser.IdObjAttContext ctx) {
    SSMObject obj = createIfNotExist(ctx.OBJECTTYPE().toString());
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
  public TreeNode visitIdAttPara(SSMRuleParser.IdAttParaContext ctx) {
    SSMObject obj = createIfNotExist("Default");
    Property p = obj.getProperty(ctx.ID().getText());
    if (p == null) {
      throw new RuleParserException("Object " + obj.toString()
          + " does not have a attribute named '" + "'" + ctx.ID().getText());
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
  public TreeNode visitIdObjAttPara(SSMRuleParser.IdObjAttParaContext ctx) {
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
  public TreeNode visitNumricexprId(SSMRuleParser.NumricexprIdContext ctx) {
    return visit(ctx.id());
  }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override
  public TreeNode visitNumricexprCurve(SSMRuleParser.NumricexprCurveContext ctx) {
    return visit(ctx.numricexpr());
  }

  // numricexpr opr numricexpr
  @Override
  public TreeNode visitNumricexprAdd(SSMRuleParser.NumricexprAddContext ctx) {
    return generalExprOpExpr(ctx);
  }

  @Override
  public TreeNode visitNumricexprMul(SSMRuleParser.NumricexprMulContext ctx) {
    return generalExprOpExpr(ctx);
  }

  @Override
  public TreeNode visitNumricexprLong(SSMRuleParser.NumricexprLongContext ctx) {
    return pharseConstLong(ctx.LONG().getText());
  }

  private TreeNode generalExprOpExpr(ParserRuleContext ctx) {
    TreeNode r1 = visit(ctx.getChild(0));
    TreeNode r2 = visit(ctx.getChild(2));
    return generalHandleExpr(ctx.getChild(1).getText(), r1, r2);
  }

  private TreeNode generalHandleExpr(String operator, TreeNode left, TreeNode right) {
    TreeNode ret = null;
    try {
      if (left.isOperNode() ||  right.isOperNode()) {
        ret = new OperNode(OperatorType.fromString(operator), left, right);
      } else if (left.eval().isConst() && right.eval().isConst()) {
        ret = new ValueNode(left.eval().eval(OperatorType.fromString(operator), right.eval()));
      } else {
        ret = new OperNode(OperatorType.fromString(operator), left, right);
      }
    } catch (IOException e) {
      // Error handle
    }
    return ret;
  }

  // bool value
  @Override
  public TreeNode visitBvAndOR(SSMRuleParser.BvAndORContext ctx) {
    return generalExprOpExpr(ctx);
  }

  @Override
  public TreeNode visitBvId(SSMRuleParser.BvIdContext ctx) {
    return visit(ctx.id());
  }

  @Override
  public TreeNode visitBvNot(SSMRuleParser.BvNotContext ctx) {
    TreeNode left = visit(ctx.boolvalue());
    // TODO: bypass null
    TreeNode right = new ValueNode(new VisitResult(ValueType.BOOLEAN, null));
    return generalHandleExpr(ctx.NOT().getText(), left, right);
  }

  @Override
  public TreeNode visitBvCurve(SSMRuleParser.BvCurveContext ctx) {
    return visit(ctx.boolvalue());
  }

  @Override
  public TreeNode visitBvCompareexpr(SSMRuleParser.BvCompareexprContext ctx) {
    return visit(ctx.compareexpr());
  }

  // Compare

  @Override
  public TreeNode visitCmpIdLong(SSMRuleParser.CmpIdLongContext ctx) {
    return generalExprOpExpr(ctx);
  }

  @Override
  public TreeNode visitCmpIdString(SSMRuleParser.CmpIdStringContext ctx) {
    return generalExprOpExpr(ctx);
  }

  @Override
  public TreeNode visitCmpIdStringMatches(SSMRuleParser.CmpIdStringMatchesContext ctx) {
    return generalExprOpExpr(ctx);
  }

  @Override
  public TreeNode visitCmpTimeintvalTimeintval(SSMRuleParser.CmpTimeintvalTimeintvalContext ctx) {
    return generalExprOpExpr(ctx);
  }

  @Override
  public TreeNode visitCmpTimepointTimePoint(SSMRuleParser.CmpTimepointTimePointContext ctx) {
    return generalExprOpExpr(ctx);
  }

  // String
  @Override
  public TreeNode visitStrPlus(SSMRuleParser.StrPlusContext ctx) {
    return generalExprOpExpr(ctx);
  }


  @Override
  public TreeNode visitStrOrdString(SSMRuleParser.StrOrdStringContext ctx) {
    return pharseConstString(ctx.STRING().getText());
  }

  @Override
  public TreeNode visitStrID(SSMRuleParser.StrIDContext ctx) {
    return visit(ctx.id());
  }

  @Override
  public TreeNode visitStrCurve(SSMRuleParser.StrCurveContext ctx) {
    return visit(ctx.getChild(1));
  }

  @Override
  public TreeNode visitStrTimePointStr(SSMRuleParser.StrTimePointStrContext ctx) {
    return new ValueNode(new VisitResult(ValueType.STRING,
        ctx.TIMEPOINTCONST().getText()));
  }

  @Override
  public TreeNode visitConstLong(SSMRuleParser.ConstLongContext ctx) {
    return pharseConstLong(ctx.getText());
  }

  @Override
  public TreeNode visitConstString(SSMRuleParser.ConstStringContext ctx) {
    return pharseConstString(ctx.getText());
  }

  @Override
  public TreeNode visitConstTimeInverval(SSMRuleParser.ConstTimeInvervalContext ctx) {
    return pharseConstTimeInterval(ctx.getText());
  }

  @Override
  public TreeNode visitConstTimePoint(SSMRuleParser.ConstTimePointContext ctx) {
    return pharseConstTimePoint(ctx.getText());
  }

  private TreeNode pharseConstTimeInterval(String str) {
    long intval = 0L;
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
      ret += " WHERE " + doGenerateSql(root, "files").getRet();
    }

    sqlStatements.add(ret);

    return new TranslateResult(sqlStatements,
        tempTableNames, dynamicParameters, sqlStatements.size() - 1);
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
        if (!lop.getTableName().equals(tableName)) {
          lop = new NodeTransResult(tableName, connectTables(tableName, lop));
        }

        if (!rop.getTableName().equals(tableName)) {
          rop = new NodeTransResult(tableName, connectTables(tableName, rop));
        }
      }

      if (optype == OperatorType.NOT) {
        return new NodeTransResult(tableName,
            op + " " + connectTables(tableName, lop));
      }
      return new NodeTransResult(
          lop.getTableName() != null ? lop.getTableName() : rop.getTableName(),
          "(" + lop.getRet() + " " + op + " " + rop.getRet() + ")");

    } else {
      ValueNode vNode = (ValueNode) root;
      VisitResult vr = vNode.eval();
      if (vr.isConst()) {
        switch (vr.getValueType()) {
          case LONG:
            return new NodeTransResult(null, "" + ((Long) vr.getValue()));
          case STRING:
            return new NodeTransResult(null,
                "'" + ((String) vr.getValue()) + "'");
          // TODO: for other types
          default:
            throw new IOException("Type = " + vr.getValueType().toString());
        }
      } else {
        PropertyRealParas realParas = vr.getRealParas();
        Property p = realParas.getProperty();
        // TODO: hard code now, abstract later
        if (p.getPropertyName() == "accessCount") {
          String virTab = "VIR_ACC_CNT_TAB_" + realParas.instId();
          tempTableNames.add(virTab);
          sqlStatements.add("DROP TABLE IF EXISTS " + virTab);
          sqlStatements.add("$@genVirtualAccessCountTable(" + virTab + ")");
          dynamicParameters.put(virTab,
              Arrays.asList(realParas.getValues(), virTab));
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
