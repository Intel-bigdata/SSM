package org.apache.hadoop.ssm.rule.parser;

/**
 * Created by root on 3/24/17.
 */
public abstract class TreeNode {
  protected TreeNode left;
  protected TreeNode right;

  public abstract boolean isLeaf();
}
