/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.protocol.protobuffer;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.security.KerberosInfo;
import org.smartdata.SmartConstants;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.protocol.ClientServerProto.GetFileStateRequestProto;
import org.smartdata.protocol.ClientServerProto.GetFileStateResponseProto;
import org.smartdata.protocol.ClientServerProto.ReportFileAccessEventRequestProto;
import org.smartdata.protocol.ClientServerProto.ReportFileAccessEventResponseProto;

@KerberosInfo(
  serverPrincipal = SmartConfKeys.SMART_SERVER_KERBEROS_PRINCIPAL_KEY)
@ProtocolInfo(protocolName = SmartConstants.SMART_CLIENT_PROTOCOL_NAME,
    protocolVersion = 1)
public interface ClientProtocolProtoBuffer {
  ReportFileAccessEventResponseProto
  reportFileAccessEvent(RpcController controller,
      ReportFileAccessEventRequestProto req) throws ServiceException;

  GetFileStateResponseProto
  getFileState(RpcController controller,
      GetFileStateRequestProto req) throws ServiceException;
}
