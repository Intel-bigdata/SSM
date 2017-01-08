package org.apache.hadoop.ssm.api

import java.io.{File, FileOutputStream, ObjectOutputStream}
import java.time.Duration
import org.apache.hadoop.ssm.Condition
import org.apache.hadoop.ssm.Property
import org.apache.hadoop.ssm.api.Expression._

object FILE {
//  def name(condition: Condition[String]): FileFilterRule[String] = {
//    new FileFilterRule(condition)
//  }

//  def accessCount(condition: Condition[Int]): PropertyFilterRule[Int] = {
//    new PropertyFilterRule(condition, Property.ACCESSCOUNT)
//  }
}

object Test extends App {
//  val condition = FILE.name(_.startsWith("test")) and
//    FILE.accessCount(_ >= 3).in(Window(Duration.ofMinutes(1), Duration.ofSeconds(10))) cache
//
//  val os = new ObjectOutputStream(new FileOutputStream(new File("/tmp/result")))
//  os.writeObject(condition)
//  os.close()
}