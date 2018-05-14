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

import com.google.protobuf.ServiceException;
import org.apache.hadoop.ipc.RPC;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.model.FileState;
import org.smartdata.protocol.ClientServerProto.DeleteFileStateRequestProto;
import org.smartdata.protocol.ClientServerProto.FileStateProto;
import org.smartdata.protocol.ClientServerProto.GetFileStateRequestProto;
import org.smartdata.protocol.ClientServerProto.GetFileStateResponseProto;
import org.smartdata.protocol.ClientServerProto.GetFileStatesRequestProto;
import org.smartdata.protocol.ClientServerProto.GetFileStatesResponseProto;
import org.smartdata.protocol.ClientServerProto.GetSmallFileListRequestProto;
import org.smartdata.protocol.ClientServerProto.GetSmallFileListResponseProto;
import org.smartdata.protocol.ClientServerProto.ReportFileAccessEventRequestProto;
import org.smartdata.protocol.ClientServerProto.UpdateFileStateRequestProto;
import org.smartdata.protocol.SmartClientProtocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClientProtocolClientSideTranslator implements
    java.io.Closeable, SmartClientProtocol {
  private ClientProtocolProtoBuffer rpcProxy;

  public ClientProtocolClientSideTranslator(
      ClientProtocolProtoBuffer proxy) {
    rpcProxy = proxy;
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
    rpcProxy = null;
  }

  @Override
  public List<String> getSmallFileList() throws IOException {
    GetSmallFileListRequestProto req = GetSmallFileListRequestProto.newBuilder().build();
    try {
      GetSmallFileListResponseProto response = rpcProxy.getSmallFileList(null, req);
      return response.getSmallFileList();
    } catch (ServiceException e) {
      throw ProtoBufferHelper.getRemoteException(e);
    }
  }

  @Override
  public void reportFileAccessEvent(FileAccessEvent event) throws IOException {
    ReportFileAccessEventRequestProto req =
        ReportFileAccessEventRequestProto.newBuilder()
            .setFilePath(event.getPath())
            .setFileId(0)
            .setAccessedBy(event.getAccessedBy())
            .build();
    try {
      rpcProxy.reportFileAccessEvent(null, req);
    } catch (ServiceException e) {
      throw ProtoBufferHelper.getRemoteException(e);
    }
  }

  @Override
  public FileState getFileState(String filePath) throws IOException {
    GetFileStateRequestProto req = GetFileStateRequestProto.newBuilder()
        .setFilePath(filePath)
        .build();
    try {
      GetFileStateResponseProto response = rpcProxy.getFileState(null, req);
      return ProtoBufferHelper.convert(response.getFileState());
    } catch (ServiceException e) {
      throw ProtoBufferHelper.getRemoteException(e);
    }
  }

  @Override
  public List<FileState> getFileStates(String filePath) throws IOException {
    GetFileStatesRequestProto req = GetFileStatesRequestProto.newBuilder()
        .setFilePath(filePath)
        .build();
    try {
      GetFileStatesResponseProto response = rpcProxy.getFileStates(null, req);
      List<FileState> fileStateList = new ArrayList<>();
      for (FileStateProto fileStateProto : response.getFileStateList()) {
        fileStateList.add(ProtoBufferHelper.convert(fileStateProto));
      }
      return fileStateList;
    } catch (ServiceException e) {
      throw ProtoBufferHelper.getRemoteException(e);
    }
  }

  @Override
  public void updateFileState(FileState fileState) throws IOException {
    UpdateFileStateRequestProto req = UpdateFileStateRequestProto.newBuilder()
        .setFileState(ProtoBufferHelper.convert(fileState))
        .build();
    try {
      rpcProxy.updateFileState(null, req);
    } catch (ServiceException e) {
      throw ProtoBufferHelper.getRemoteException(e);
    }
  }

  @Override
  public void deleteFileState(String filePath, boolean recursive) throws IOException {
    DeleteFileStateRequestProto req = DeleteFileStateRequestProto.newBuilder()
        .setFilePath(filePath)
        .setRecursive(recursive)
        .build();
    try {
      rpcProxy.deleteFileState(null, req);
    } catch (ServiceException e) {
      throw ProtoBufferHelper.getRemoteException(e);
    }
  }
}
