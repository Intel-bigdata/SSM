package org.smartdata.model;

import org.smartdata.model.ActionInfo;

import java.util.Comparator;

public class ActionInfoComparator implements Comparator<ActionInfo> {
  @Override
  public int compare(ActionInfo o1, ActionInfo o2) {
    return (int) (o1.getCreateTime() - o2.getCreateTime());
  }
}
