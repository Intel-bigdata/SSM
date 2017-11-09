/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.smartdata.utils;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import java.io.File;
import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Jaas utilities for Smart login.
 */
public class SecurityUtil {
  public static final Logger LOG = LoggerFactory.getLogger(SecurityUtil.class);

  public static final boolean ENABLE_DEBUG = true;

  private static String getKrb5LoginModuleName() {
    return System.getProperty("java.vendor").contains("IBM")
        ? "com.ibm.security.auth.module.Krb5LoginModule"
        : "com.sun.security.auth.module.Krb5LoginModule";
  }

  /**
   * Log a user in from a tgt ticket.
   *
   * @throws IOException
   */
  public static synchronized Subject loginUserFromTgtTicket(String smartSecurity)
      throws IOException {

    TICKET_KERBEROS_OPTIONS.put("smartSecurity", smartSecurity);
    Subject subject = new Subject();
    Configuration conf = new SmartJaasConf();
    String confName = "ticket-kerberos";
    LoginContext loginContext = null;
    try {
      loginContext = new LoginContext(confName, subject, null, conf);
    } catch (LoginException e) {
      throw new IOException("Fail to create LoginContext for " + e);
    }
    try {
      loginContext.login();
      LOG.info("Login successful for user " + subject.getPrincipals().iterator().next().getName());
    } catch (LoginException e) {
      throw new IOException("Login failure for " + e);
    }
    return loginContext.getSubject();
  }

  /**
   * Smart Jaas config.
   */
  static class SmartJaasConf extends Configuration {
    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {

      return new AppConfigurationEntry[]{
          TICKET_KERBEROS_LOGIN};
    }
  }

  private static final Map<String, String> BASIC_JAAS_OPTIONS =
      new HashMap<String, String>();

  static {
    String jaasEnvVar = System.getenv("SMART_JAAS_DEBUG");
    if (jaasEnvVar != null && "true".equalsIgnoreCase(jaasEnvVar)) {
      BASIC_JAAS_OPTIONS.put("debug", String.valueOf(ENABLE_DEBUG));
    }
  }

  private static final Map<String, String> TICKET_KERBEROS_OPTIONS =
      new HashMap<String, String>();

  static {
    TICKET_KERBEROS_OPTIONS.put("doNotPrompt", "true");
    TICKET_KERBEROS_OPTIONS.put("useTgtTicket", "true");
    TICKET_KERBEROS_OPTIONS.putAll(BASIC_JAAS_OPTIONS);
  }

  private static final AppConfigurationEntry TICKET_KERBEROS_LOGIN =
      new AppConfigurationEntry(getKrb5LoginModuleName(),
          AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL,
          TICKET_KERBEROS_OPTIONS);

  public static Subject loginUsingTicketCache(String principal) throws IOException {
    String ticketCache = System.getenv("KRB5CCNAME");
    return loginUsingTicketCache(principal, ticketCache);
  }

  @VisibleForTesting
  static Subject loginUsingTicketCache(
      String principal, String ticketCacheFileName) throws IOException {
    Set<Principal> principals = new HashSet<Principal>();
    principals.add(new KerberosPrincipal(principal));

    Subject subject = new Subject(false, principals,
        new HashSet<Object>(), new HashSet<Object>());

    Configuration conf = useTicketCache(principal, ticketCacheFileName);
    String confName = "TicketCacheConf";
    LoginContext loginContext = null;
    try {
      loginContext = new LoginContext(confName, subject, null, conf);
    } catch (LoginException e) {
      throw new IOException("Fail to create LoginContext for " + e);
    }
    try {
      loginContext.login();
      LOG.info("Login successful for user "
          + subject.getPrincipals().iterator().next().getName());
    } catch (LoginException e) {
      throw new IOException("Login failure for " + e);
    }
    return loginContext.getSubject();
  }

  public static void loginUsingKeytab(SmartConf conf) throws IOException{
    String keytabFilename = conf.get(SmartConfKeys.SMART_SERVER_KEYTAB_FILE_KEY);
    if (keytabFilename == null || keytabFilename.length() == 0) {
      throw new IOException("Running in secure mode, but config doesn't have a keytab");
    }
    File keytabPath = new File(keytabFilename);
    String principal = conf.get(SmartConfKeys.SMART_SERVER_KERBEROS_PRINCIPAL_KEY);
    try {
      SecurityUtil.loginUsingKeytab(principal, keytabPath.getAbsolutePath());
    } catch (IOException e) {
      LOG.error("Fail to login using keytab. " + e);
      throw new IOException("Fail to login using keytab. " + e);
    }
    LOG.info("Login successful for user: " + principal);
  }

  public static void loginUsingKeytab(String keytabFilename, String principal)
      throws IOException {

    if (keytabFilename == null || keytabFilename.length() == 0) {
      throw new IOException("Running in secure mode, but config doesn't have a keytab");
    }
    File keytabPath = new File(keytabFilename);

    try {
      UserGroupInformation.loginUserFromKeytab(principal, keytabPath.getAbsolutePath());
    } catch (IOException e) {
      LOG.error("Fail to login using keytab. " + e);
      throw new IOException("Fail to login using keytab. " + e);
    }
    LOG.info("Login successful for user: " + principal);
  }

  public static Subject loginUsingKeytab(
      String principal, File keytabFile) throws IOException {
    Set<Principal> principals = new HashSet<Principal>();
    principals.add(new KerberosPrincipal(principal));

    Subject subject = new Subject(false, principals,
        new HashSet<Object>(), new HashSet<Object>());

    Configuration conf = useKeytab(principal, keytabFile);
    String confName = "KeytabConf";
    LoginContext loginContext = null;
    try {
      loginContext = new LoginContext(confName, subject, null, conf);
      LOG.info("Login successful for user "
          + subject.getPrincipals().iterator().next().getName());
    } catch (LoginException e) {
      throw new IOException("Faill to create LoginContext for " + e);
    }
    try {
      loginContext.login();
    } catch (LoginException e) {
      throw new IOException("Login failure for " + e);
    }
    return loginContext.getSubject();
  }

  public static Configuration useTicketCache(String principal, String ticketCacheFileName) {
    return new TicketCacheJaasConf(principal, ticketCacheFileName);
  }

  public static Configuration useKeytab(String principal, File keytabFile) {
    return new KeytabJaasConf(principal, keytabFile);
  }

  static class TicketCacheJaasConf extends Configuration {
    private String principal;
    private String ticketCacheFileName;

    TicketCacheJaasConf(String principal, String ticketCacheFileName) {
      this.principal = principal;
      this.ticketCacheFileName = ticketCacheFileName;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      Map<String, String> options = new HashMap<String, String>();
      options.put("principal", principal);
      options.put("storeKey", "false");
      options.put("doNotPrompt", "false");
      options.put("useTicketCache", "true");
      options.put("renewTGT", "true");
      options.put("refreshKrb5Config", "true");
      options.put("isInitiator", "true");

      if (ticketCacheFileName != null) {
        if (System.getProperty("java.vendor").contains("IBM")) {
          // The first value searched when "useDefaultCcache" is used.
          System.setProperty("KRB5CCNAME", ticketCacheFileName);
        } else {
          options.put("ticketCache", ticketCacheFileName);
        }
      }
      options.putAll(BASIC_JAAS_OPTIONS);

      return new AppConfigurationEntry[]{
          new AppConfigurationEntry(getKrb5LoginModuleName(),
              AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
              options)};
    }
  }

  static class KeytabJaasConf extends Configuration {
    private String principal;
    private File keytabFile;

    KeytabJaasConf(String principal, File keytab) {
      this.principal = principal;
      this.keytabFile = keytab;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      Map<String, String> options = new HashMap<String, String>();
      options.put("keyTab", keytabFile.getAbsolutePath());
      options.put("principal", principal);
      options.put("useKeyTab", "true");
      options.put("storeKey", "true");
      options.put("doNotPrompt", "true");
      options.put("renewTGT", "false");
      options.put("refreshKrb5Config", "true");
      options.put("isInitiator", "true");
      options.putAll(BASIC_JAAS_OPTIONS);

      return new AppConfigurationEntry[]{
          new AppConfigurationEntry(getKrb5LoginModuleName(),
              AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
              options)};
    }
  }

  public static boolean isSecurityEnabled(SmartConf conf) {
    return conf.getBoolean(SmartConfKeys.SMART_SECURITY_ENABLE, false);
  }
}
