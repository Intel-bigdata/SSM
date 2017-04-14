package org.apache.hadoop.ssm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ssm.protocol.SSMClient;
import org.junit.Test;

import static org.junit.Assert.*;

import java.net.InetSocketAddress;

public class SSMClientTest {

  @Test
  public void test() throws Exception {

    String ADDRESS = "localhost";
    int port = 9998;
    InetSocketAddress addr = new InetSocketAddress(ADDRESS, port);
    SSMServer.createSSM(null, new Configuration());
    SSMClient ssmClient = new SSMClient(new Configuration(), addr);
    String state = ssmClient.getServiceStatus().getState().name();
    boolean isStateExpected;
    if ("SAFEMODE".equals(state) || "ACTIVE".equals(state)) isStateExpected = true;
    else isStateExpected = false;
    assertTrue(isStateExpected);

    assertSame(false, ssmClient.getServiceStatus().getisActive());
  }

}