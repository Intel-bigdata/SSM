package org.apache.hadoop.ssm.api

import org.apache.hadoop.ssm._
import org.apache.hadoop.ssm.Property
import org.apache.hadoop.ssm.Action
import java.time.Duration

object Expression {
  val folderPattern = "^(/[a-zA-Z0-9]+)+".r

  sealed trait Operator

  case object AND extends Operator

  case object OR extends Operator

  case class TreeNode(value: Operator, left: TreeNode, right: TreeNode){
    def and(other: TreeNode): TreeNode = {
      new TreeNode(AND, this, other)
    }

    def or(other: TreeNode): TreeNode = {
      new TreeNode(OR, this, other)
    }

    def cache: SSMRule = {
      new SSMRule(null, this, Action.CACHE)
    }
  }

  case class SSMRule(fileFilterRule: FileFilterRule, root: TreeNode, action: Action) {
    def getId(): Long = {
      this.hashCode()
    }
  }

  sealed trait PropertyManipulation

  case object Historical extends PropertyManipulation

  case class Window(size: Duration, step: Duration) extends PropertyManipulation

  abstract class Rule[T] extends Operator {
    def meetCondition(arg: T): Boolean
  }

  case class FileFilterRule(pathPattern: String) extends Rule[String] {
    def getPrefix(): String = {
      folderPattern.findFirstIn(pathPattern).getOrElse("")
    }

    def meetCondition(arg: String): Boolean = {
      pathPattern.r.pattern.matcher(arg).matches()
    }
  }

  case class PropertyFilterRule[T](condition: Condition[T], threshold: T, property: Property,
    propertyManipulation: PropertyManipulation = Historical) extends Rule[T] {

    def in(stateManipulation: PropertyManipulation): PropertyFilterRule[T] = {
      new PropertyFilterRule(condition, threshold, property, stateManipulation)
    }

    def meetCondition(arg: T): Boolean = {
      condition(arg)
    }
  }

  object Rule {
    implicit def ruleToTreenode[T](rule: Rule[T]): TreeNode = {
      new TreeNode(rule, null, null)
    }
  }
}
