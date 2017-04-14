package org.apache.hadoop.ssm.protocolPB;

import com.google.protobuf.RpcController;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ssm.protocol.ClientSSMProto;

@ProtocolInfo(protocolName = "org.apache.hadoop.ssm.protocolPB.ClientSSMProtocolPB",
        protocolVersion = 1)
public interface ClientSSMProtocolPB {
  public ClientSSMProto.AddResult add(RpcController controller, ClientSSMProto.AddParameters p);
  public ClientSSMProto.StatusResult getServiceStatus(RpcController controller, ClientSSMProto.StatusPara p);
}