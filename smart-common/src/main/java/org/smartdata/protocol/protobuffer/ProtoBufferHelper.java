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
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.model.ActionDescriptor;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CmdletState;
import org.smartdata.model.CompactFileState;
import org.smartdata.model.CompressionFileState;
import org.smartdata.model.FileContainerInfo;
import org.smartdata.model.FileState;
import org.smartdata.model.NormalFileState;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.RuleState;
import org.smartdata.protocol.AdminServerProto.ActionDescriptorProto;
import org.smartdata.protocol.AdminServerProto.ActionInfoProto;
import org.smartdata.protocol.AdminServerProto.ActionInfoProto.Builder;
import org.smartdata.protocol.AdminServerProto.CmdletInfoProto;
import org.smartdata.protocol.AdminServerProto.RuleInfoProto;
import org.smartdata.protocol.ClientServerProto.CompactFileStateProto;
import org.smartdata.protocol.ClientServerProto.CompressionFileStateProto;
import org.smartdata.protocol.ClientServerProto.FileStateProto;
import org.smartdata.protocol.ClientServerProto.ReportFileAccessEventRequestProto;
import org.smartdata.protocol.ClientServerProto.S3FileStateProto;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

public class ProtoBufferHelper {
  private ProtoBufferHelper() {
  }

  public static IOException getRemoteException(ServiceException se) {
    Throwable e = se.getCause();
    if (e == null) {
      return new IOException(se);
    }
    return e instanceof IOException ? (IOException) e : new IOException(se);
  }

  public static int convert(RuleState state) {
    return state.getValue();
  }

  public static RuleState convert(int state) {
    return RuleState.fromValue(state);
  }

  public static RuleInfoProto convert(RuleInfo info) {
    return RuleInfoProto.newBuilder().setId(info.getId())
        .setSubmitTime(info.getSubmitTime())
        .setLastCheckTime(info.getLastCheckTime())
        .setRuleText(info.getRuleText())
        .setNumChecked(info.getNumChecked())
        .setNumCmdsGen(info.getNumCmdsGen())
        .setRulestateProto(convert(info.getState())).build();
  }

  public static RuleInfo convert(RuleInfoProto proto) {
    return RuleInfo.newBuilder().setId(proto.getId())
        .setSubmitTime(proto.getSubmitTime())
        .setLastCheckTime(proto.getLastCheckTime())
        .setRuleText(proto.getRuleText())
        .setNumChecked(proto.getNumChecked())
        .setNumCmdsGen(proto.getNumCmdsGen())
        .setState(convert(proto.getRulestateProto())).build();
  }

  public static CmdletInfo convert(CmdletInfoProto proto) {
    // TODO replace actionType with aids
    CmdletInfo.Builder builder = CmdletInfo.newBuilder();
    builder.setCid(proto.getCid())
        .setRid(proto.getRid())
        .setState(CmdletState.fromValue(proto.getState()))
        .setParameters(proto.getParameters())
        .setGenerateTime(proto.getGenerateTime())
        .setStateChangedTime(proto.getStateChangedTime());
    List<Long> list = proto.getAidsList();
    builder.setAids(list);
    return builder.build();
  }

  public static CmdletInfoProto convert(CmdletInfo info) {
    // TODO replace actionType with aids
    CmdletInfoProto.Builder builder = CmdletInfoProto.newBuilder();
    builder.setCid(info.getCid())
        .setRid(info.getRid())
        .setState(info.getState().getValue())
        .setParameters(info.getParameters())
        .setGenerateTime(info.getGenerateTime())
        .setStateChangedTime(info.getStateChangedTime());
    builder.addAllAids(info.getAids());
    return builder.build();
  }

  public static ReportFileAccessEventRequestProto convert(FileAccessEvent event) {
    return ReportFileAccessEventRequestProto.newBuilder()
        .setFilePath(event.getPath())
        .setAccessedBy(event.getAccessedBy())
        .setFileId(event.getFileId())
        .build();
  }

  public static ActionInfoProto convert(ActionInfo actionInfo) {
    Builder builder = ActionInfoProto.newBuilder();
    builder.setActionName(actionInfo.getActionName())
        .setResult(actionInfo.getResult())
        .setLog(actionInfo.getLog())
        .setSuccessful(actionInfo.isSuccessful())
        .setCreateTime(actionInfo.getCreateTime())
        .setFinished(actionInfo.isFinished())
        .setFinishTime(actionInfo.getFinishTime())
        .setProgress(actionInfo.getProgress())
        .setActionId(actionInfo.getActionId())
        .setCmdletId(actionInfo.getCmdletId());
    builder.addAllArgs(CmdletDescriptor.toArgList(actionInfo.getArgs()));
    return builder.build();
  }

  public static ActionInfo convert(ActionInfoProto infoProto) {
    ActionInfo.Builder builder = ActionInfo.newBuilder();
    builder.setActionName(infoProto.getActionName())
        .setResult(infoProto.getResult())
        .setLog(infoProto.getLog())
        .setSuccessful(infoProto.getSuccessful())
        .setCreateTime(infoProto.getCreateTime())
        .setFinished(infoProto.getFinished())
        .setFinishTime(infoProto.getFinishTime())
        .setProgress(infoProto.getProgress())
        .setActionId(infoProto.getActionId())
        .setCmdletId(infoProto.getCmdletId());
    List<String> list = infoProto.getArgsList();
    try {
      builder.setArgs(CmdletDescriptor.toArgMap(list));
    } catch (ParseException e) {
      return null;
    }
    return builder.build();
  }


  public static FileAccessEvent convert(final ReportFileAccessEventRequestProto event) {
    return new FileAccessEvent(event.getFilePath(), 0, event.getAccessedBy());
  }

  public static ActionDescriptor convert(ActionDescriptorProto proto) {
    return ActionDescriptor.newBuilder()
        .setActionName(proto.getActionName())
        .setComment(proto.getComment())
        .setDisplayName(proto.getDisplayName())
        .setUsage(proto.getUsage())
        .build();
  }

  public static ActionDescriptorProto convert(ActionDescriptor ac) {
    return ActionDescriptorProto.newBuilder()
        .setActionName(ac.getActionName())
        .setComment(ac.getComment())
        .setDisplayName(ac.getDisplayName())
        .setUsage(ac.getUsage())
        .build();
  }

  private static FileContainerInfo convert(CompactFileStateProto proto) {
    String containerFilePath = proto.getContainerFilePath();
    long offset = proto.getOffset();
    long length = proto.getLength();
    return new FileContainerInfo(containerFilePath, offset, length);
  }

  public static FileState convert(FileStateProto proto) {
    FileState fileState = null;
    String path = proto.getPath();
    FileState.FileType type = FileState.FileType.fromValue(proto.getType());
    /// Unusable temporarily
    // FileState.FileStage stage = FileState.FileStage.fromValue(proto.getStage());
    if (type == null) {
      return new NormalFileState(path);
    }
    switch (type) {
      case NORMAL:
        fileState = new NormalFileState(path);
        break;
      case COMPACT:
        CompactFileStateProto compactProto = proto.getCompactFileState();
        fileState = new CompactFileState(path, convert(compactProto));
        break;
      case COMPRESSION:
        CompressionFileStateProto compressionProto = proto.getCompressionFileState();
        // convert to CompressionFileState
        fileState = convert(path, stage, compressionProto);
        break;
      case S3:
        S3FileStateProto s3Proto = proto.getS3FileState();
        // convert to S3FileState
        // fileState = convert(path, type, stage, s3Proto);
        break;
      default:
    }
    return fileState;
  }

  public static FileStateProto convert(FileState fileState) {
    FileStateProto.Builder builder = FileStateProto.newBuilder();
    builder.setPath(fileState.getPath())
        .setType(fileState.getFileType().getValue())
        .setStage(fileState.getFileStage().getValue());
    if (fileState instanceof CompactFileState) {
      FileContainerInfo fileContainerInfo = (
          (CompactFileState) fileState).getFileContainerInfo();
      builder.setCompactFileState(CompactFileStateProto.newBuilder()
          .setContainerFilePath(fileContainerInfo.getContainerFilePath())
          .setOffset(fileContainerInfo.getOffset())
          .setLength(fileContainerInfo.getLength()));
    } else if (fileState instanceof CompressionFileState) {
      builder.setCompressionFileState(convert((CompressionFileState) fileState));
    }
    /*else if (fileState instanceof S3FileState) {
      builder.setS3FileState();
    } else if (fileState instanceof ) {
    }
    */
    return builder.build();
  }

  public static CompressionFileState convert(String path,
      FileState.FileStage stage, CompressionFileStateProto proto) {
    CompressionFileState.Builder builder = CompressionFileState.newBuilder();
    builder.setFileName(path)
        .setFileStage(stage)
        .setBufferSize(proto.getBufferSize())
        .setCompressImpl(proto.getCompressionImpl())
        .setOriginalLength(proto.getOriginalLength())
        .setCompressedLength(proto.getCompressedLength())
        .setOriginalPos(proto.getOriginalPosList())
        .setCompressedPos(proto.getCompressedPosList());
    return builder.build();
  }

  public static CompressionFileStateProto convert(CompressionFileState fileState) {
    CompressionFileStateProto.Builder builder = CompressionFileStateProto.newBuilder();
    builder.setBufferSize(fileState.getBufferSize())
        .setCompressionImpl(fileState.getCompressionImpl())
        .setOriginalLength(fileState.getOriginalLength())
        .setCompressedLength(fileState.getCompressedLength());
    builder.addAllOriginalPos(Arrays.asList(fileState.getOriginalPos()));
    builder.addAllCompressedPos(Arrays.asList(fileState.getCompressedPos()));
    return builder.build();
  }
}
