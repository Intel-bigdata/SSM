package org.apache.hadoop.ssm;

/**
 * This class contains the configure keys needed by SSM.
 */
public class SSMConfigureKeys {
  public final static String DFS_SSM_ENABLED_KEY = "dfs.ssm.enabled";

  //ssm
  public final static String DFS_SSM_HTTP_ADDRESS_KEY = "dfs.ssm.http-address";
  public final static String DFS_SSM_HTTPS_ADDRESS_KEY = "dfs.ssm.https-address";
  public final static int DFS_SSM_HTTPS_PORT_DEFAULT = 9971;
  public final static int DFS_SSM_RPC_PROT_DEFAULT = 7042;
}
