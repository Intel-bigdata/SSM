package org.apache.hadoop.ssm.protocolPB;

import org.apache.hadoop.ssm.protocol.ClientSSMProto;
import org.apache.hadoop.ssm.protocol.ClientSSMProtocol;
import org.apache.hadoop.ssm.protocol.HAServiceStatus;

public class ClientSSMProtocolClientSideTranslatorPB implements ClientSSMProtocol {
  final private ClientSSMProtocolPB rpcProxy;

  public ClientSSMProtocolClientSideTranslatorPB(ClientSSMProtocolPB proxy) {
    this.rpcProxy = proxy;

  }

  public int add(int para1, int para2) {
    // TODO Auto-generated method stub
    ClientSSMProto.AddParameters req = ClientSSMProto.AddParameters.newBuilder()
            .setPara1(para1).setPara2(para2).build();
    return rpcProxy.add(null, req).getResult();
  }

  @Override
  public HAServiceStatus getServiceStatus() {
    ClientSSMProto.StatusPara req = ClientSSMProto.StatusPara.newBuilder().build();
    String state = rpcProxy.getServiceStatus(null,req).getHAServiceState();
    boolean isActive = rpcProxy.getServiceStatus(null,req).getIsActive();
    HAServiceStatus haServiceStatus;
    if (state!=null&&state.equals(HAServiceStatus.HAServiceState.ACTIVE)){
      haServiceStatus = new HAServiceStatus(HAServiceStatus.HAServiceState.ACTIVE);
    }else {
      haServiceStatus = new HAServiceStatus(HAServiceStatus.HAServiceState.SAFEMODE);
    }
    haServiceStatus.setisActive(isActive);
    return  haServiceStatus;
  }

}