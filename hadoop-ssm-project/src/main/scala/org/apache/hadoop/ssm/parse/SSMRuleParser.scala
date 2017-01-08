package org.apache.hadoop.ssm.parse

import java.time.Duration

import org.apache.hadoop.ssm.api.Expression._
import org.apache.hadoop.ssm.Condition
import scala.util.parsing.combinator.syntactical.StandardTokenParsers

import org.apache.hadoop.ssm.Action
import org.apache.hadoop.ssm.Property

object SSMRuleParser extends StandardTokenParsers{
  lexical.delimiters += (".", ":", " ", "(", ")", "|", ">=", "<=", "<", ">", "==", "!=")
  lexical.reserved += ("file", "path", "matches")

  def parseTime(time: Int, unit: String): Duration = {
    unit match {
      case "min" => Duration.ofMinutes(time)
      case "sec" => Duration.ofSeconds(time)
      case "hour" => Duration.ofHours(time)
      case "day" => Duration.ofDays(time)
    }
  }

  def parse: Parser[SSMRule] =
    fileFilter ~ ":" ~ propertyExpression ~ "|" ~ action ^^ {
      case filter ~ ":" ~ tree ~ "|" ~ act => new SSMRule(filter, tree, act)
    }

  def fileFilter: Parser[FileFilterRule] =
    "file" ~ "path" ~ "matches" ~ "(" ~ stringLit ~ ")" ^^ {
      case "file" ~ "path" ~ "matches" ~ "(" ~ regex ~ ")" => new FileFilterRule(regex)
    }

  def propertyExpression: Parser[TreeNode] =
    propertyRule ~ opt(operator ~ propertyExpression) ^^ {
      case p ~ None => TreeNode(p, null, null)
      case p ~ Some(op ~ e) => TreeNode(op, p, e)
    }

  def propertyRule: Parser[PropertyFilterRule[_ <: Any]] =
    property ~ opt(time) ~ numericExpression ^^ {
      case p ~ None ~ condition => PropertyFilterRule(condition._1, condition._2, p)
      case p ~ Some(t) ~ condition => PropertyFilterRule(condition._1, condition._2, p, Window(t, Duration.ofMinutes(1)))
    }

  def action: Parser[Action] =
    ident ^^ { case action => Action.getActionType(action) }

  def property: Parser[Property] =
    ident ^^ { case property => Property.getPropertyType(property) }

  def numericExpression: Parser[(Condition[Long], Long)] =
    ("<" | "<=" | ">=" | ">" | "==" | "!=") ~
      (numericLit ^^ {case n => n.toLong} | time ^^ {case t => t.toMillis}) ^^ {
      case ">=" ~ num => ((arg: Long) => arg >= num, num)
      case ">" ~ num => ((arg: Long) => arg > num, num)
      case "<=" ~ num => ((arg: Long) => arg <= num, num)
      case "<" ~ num => ((arg: Long) => arg < num, num)
      case "==" ~ num => ((arg: Long) => arg == num, num)
      case "!=" ~ num => ((arg: Long) => arg != num, num)
    }

  def operator: Parser[Operator] = {
    ident ^^ {
      case "and" => AND
      case "or" => OR
    }
  }

  def time: Parser[Duration] =
    "(" ~ numericLit ~ ident ~ ")" ^^ {
      case "(" ~ amount ~ timeUnit ~ ")" => parseTime(amount.toInt, timeUnit)
    }

  def parseAll(rule: String) = {
    parse(new lexical.Scanner(rule.replace(".", " ")))
  }
}

object Test extends App {
  val result = SSMRuleParser.parseAll("file.path matches('abc*') : age >= 10 | cache")
  //println(result.get.root.value.asInstanceOf[PropertyFilterRule[Long]].threshold)

  val pathPattern = "^(/[a-zA-Z0-9]+)+".r
  val path = "/root/user/huafengw/[a-zA-Z0-9]*"
  println(pathPattern.findFirstIn(path))
}
