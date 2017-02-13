package org.apache.hadoop.ssm;

/**
 * Created by root on 2/10/17.
 */
public abstract class HttpOp {
  public static final String NAME = "SSMHttpOp";

  public enum Type {
    GET, PUT
  }

  public interface Op {
    Type getType();
  }

  public Op getOp() {
    return null;
  }

  public String getName() {
    return NAME;
  }
}
