package org.apache.hadoop.ssm.rule.parser;

import java.io.IOException;

/**
 * Created by root on 3/24/17.
 */
public class OperNode extends TreeNode {
  private OperatorType operatorType;


  public OperNode(OperatorType type, TreeNode left, TreeNode right) {
    operatorType = type;
    this.left = left;
    this.right = right;
  }

  // return this node's return type
  public ValueType getValueType() {

  }

  public VisitResult eval() throws IOException {
    if (left.isLeaf() && (right == null || right.isLeaf())) {
    }
  }

  public static VisitResult eval(TreeNode root) throws IOException {
    if (root == null) {
      return null;
    } else if (root.isLeaf()) {
      return (VisitResult) root;
    } else if (root.left.isLeaf())
      ((VisitResult) (root.left)).eval()
    }
  }

  public boolean isLeaf() {
    return false;
  }
}
