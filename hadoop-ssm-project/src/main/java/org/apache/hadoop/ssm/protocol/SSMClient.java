package org.apache.hadoop.ssm.protocol;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ssm.protocolPB.ClientSSMProtocolClientSideTranslatorPB;
import org.apache.hadoop.ssm.protocolPB.ClientSSMProtocolPB;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by hadoop on 17-4-7.
 */
public class SSMClient {
  final static long VERSION = 1;
  Configuration conf;
  ClientSSMProtocol ssm;

  public boolean isClientRunning() {
    return clientRunning;
  }

  volatile boolean clientRunning = true;

  public SSMClient(Configuration conf, InetSocketAddress address) throws IOException {
    this.conf = conf;
    RPC.setProtocolEngine(conf, ClientSSMProtocolPB.class, ProtobufRpcEngine.class);
    ClientSSMProtocolPB proxy = RPC.getProxy(ClientSSMProtocolPB.class, VERSION, address, conf);
    ClientSSMProtocol clientSSMProtocol = new ClientSSMProtocolClientSideTranslatorPB(proxy);
    this.ssm = clientSSMProtocol;
  }

  public int add(int para1, int para2) {
    return ssm.add(para1, para2);
  }

  public HAServiceStatus getServiceStatus() {
    return ssm.getServiceStatus();
  }
}
