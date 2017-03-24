package org.apache.hadoop.ssm.rule.parser;

/**
 * Created by root on 3/24/17.
 */
public enum OperatorType {
    ADD(false), // "+"
    SUB(false),
    MUL(false),
    DIV(false),
    GT(true), // ">"
    GE(true), // ">="
    LT(true), // "<"
    LE(true), // "<="
    EQ(true), // "=="
    NE(true), // "!="
    AND(true),
    OR(true),
    NOT(true)
    ;

    private boolean isCompare;

    private OperatorType(boolean isCompare) {
      this.isCompare = isCompare;
    }
}
