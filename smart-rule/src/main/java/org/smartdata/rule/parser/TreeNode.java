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

import java.io.IOException;

public abstract class TreeNode {
  protected TreeNode parent;
  protected TreeNode left;
  protected TreeNode right;

  public TreeNode(TreeNode left, TreeNode right) {
    this.left = left;
    this.right = right;
  }

  public abstract VisitResult eval() throws IOException;

  public abstract ValueType getValueType();

  // TODO: print the tree
  //public String toString();

  public abstract void checkValueType() throws IOException;

  public TreeNode getLeft() {
    return left;
  }

  public TreeNode getRight() {
    return right;
  }

  public void setParent(TreeNode parent) {
    this.parent = parent;
  }

  public TreeNode getParent() {
    return parent;
  }

  public TreeNode getPeer() {
    if (getParent() == null) {
      return null;
    }

    return getParent().getLeft() == this ? getParent().getRight() : getParent().getRight();
  }

  public abstract boolean isOperNode();
}
