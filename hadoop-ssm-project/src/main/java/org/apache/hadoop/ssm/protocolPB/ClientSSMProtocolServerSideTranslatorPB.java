package org.apache.hadoop.ssm.protocolPB;

import com.google.protobuf.RpcController;
import org.apache.hadoop.ssm.protocol.*;

public class ClientSSMProtocolServerSideTranslatorPB implements ClientSSMProtocolPB,
        ClientSSMProto.StatusService.BlockingInterface {
  final private ClientSSMProtocol server;

  public ClientSSMProtocolServerSideTranslatorPB(ClientSSMProtocol server) {
    this.server = server;
  }

  @Override
  public ClientSSMProto.StatusResult getServiceStatus(RpcController controller, ClientSSMProto.StatusPara request){
    ClientSSMProto.StatusResult.Builder builder = ClientSSMProto.StatusResult.newBuilder();
    HAServiceStatus haServiceStatus = server.getServiceStatus();
    builder.setHAServiceState(haServiceStatus.getState().name());
    builder.setIsActive(haServiceStatus.getisActive());
    return builder.build();
  }

  public ClientSSMProto.AddResult add(RpcController controller, ClientSSMProto.AddParameters p) {
    // TODO Auto-generated method stub
    ClientSSMProto.AddResult.Builder builder = ClientSSMProto.AddResult
            .newBuilder();
    int result = server.add(p.getPara1(), p.getPara2());
    builder.setResult(result);
    return builder.build();
  }

}