package org.smartdata.utils;

import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.apache.kerby.kerberos.kerb.type.ticket.TgtTicket;
import org.apache.kerby.util.NetworkUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.security.auth.Subject;

import java.io.File;

/**
 * Test for JaasLoginUtil.
 */
public class TestSecurityUtil {
  private SimpleKdcServer kdcServer;
  private String serverHost = "localhost";
  private int serverPort = -1;

  private final String keytabFileName = "smart.keytab";
  private final String principal = "ssmroot@EXAMPLE.COM";
  private final String ticketCacheFileName = "smart.cc";

  @Before
  public void setupKdcServer() throws Exception {
    kdcServer = new SimpleKdcServer();
    kdcServer.setKdcHost(serverHost);
    kdcServer.setAllowUdp(false);
    kdcServer.setAllowTcp(true);
    serverPort = NetworkUtil.getServerPort();
    kdcServer.setKdcTcpPort(serverPort);
    kdcServer.init();
    kdcServer.start();
  }

  private File generateKeytab(String keytabFileName, String principal) throws Exception {
    File keytabFile = new File(keytabFileName);
    kdcServer.createAndExportPrincipals(keytabFile, principal);
    return new File(keytabFileName);
  }

  @Test
  public void loginUsingKeytab() throws Exception {
    File keytabFile = generateKeytab(keytabFileName, principal);
    Subject subject = SecurityUtil.loginUsingKeytab(principal, keytabFile);
    Assert.assertEquals(principal, subject.getPrincipals().iterator().next().getName());
    System.out.println("Login successful for user: "
        + subject.getPrincipals().iterator().next());
  }

  @Test
  public void loginUsingTicket() throws Exception {
    File keytabFile = generateKeytab(keytabFileName, principal);
    TgtTicket tgtTicket = kdcServer.getKrbClient().requestTgt(principal, keytabFile);
    File ticketCacheFile = new File(ticketCacheFileName);
    kdcServer.getKrbClient().storeTicket(tgtTicket, ticketCacheFile);
    Subject subject = SecurityUtil.loginUsingTicketCache(principal, ticketCacheFileName);
    Assert.assertEquals(principal, subject.getPrincipals().iterator().next().getName());
    System.out.println("Login successful for user: "
        + subject.getPrincipals().iterator().next());
  }

  @After
  public void tearDown() throws Exception {
    File keytabFile = new File(keytabFileName);
    if (keytabFile.exists()) {
      keytabFile.delete();
    }
    File ticketCacheFile = new File(ticketCacheFileName);
    if (ticketCacheFile.exists()) {
      ticketCacheFile.delete();
    }
    if (kdcServer != null) {
      kdcServer.stop();
    }
  }
}
