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
package org.smartdata.alluxio.metric.fetcher;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.proto.journal.File.*;
import alluxio.proto.journal.Journal.JournalEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.alluxio.AlluxioUtil;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.FileDiff;
import org.smartdata.model.FileDiffType;
import org.smartdata.model.FileInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AlluxioEntryApplier {
  
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioEntryApplier.class);

  private final MetaStore metaStore;
  private FileSystem fs;
  
  public AlluxioEntryApplier(MetaStore metaStore, FileSystem fs) {
    this.metaStore = metaStore;
    this.fs = fs;
  }

  public void apply(JournalEntry entry) throws IOException, MetaStoreException {
    List<String> statements = new ArrayList<>();
    List<String> sqlist = processEntryToSql(entry);
    if (sqlist != null && !sqlist.isEmpty()){
      for (String sql : sqlist) {
        if (sql != null && sql.length() > 0) {
          statements.add(sql);
        }
      }
    }
    this.metaStore.execute(statements);
  }

  private List<String> processEntryToSql(JournalEntry entry) throws IOException, MetaStoreException {
    if (entry.hasInodeDirectory()) {
      LOG.trace("entry type:" + entry.getInodeDirectory().getClass() +
          ", id:" + entry.getInodeDirectory().getId());
      InodeDirectoryEntry inodeDirectoryEntry = entry.getInodeDirectory();
      String inodeDir = getPathFromInodeDir(inodeDirectoryEntry);
      URIStatus dStatus = null;
      try {
        dStatus = fs.getStatus(new AlluxioURI(inodeDir));
      } catch (AlluxioException e) {
        e.printStackTrace();
      }
      FileInfo fileInfo = AlluxioUtil.convertFileStatus(dStatus);
      metaStore.insertFile(fileInfo);
      return Collections.singletonList("");
    } else if (entry.hasInodeFile()) {
      LOG.trace("entry type:" + entry.getInodeFile().getClass() +
          ", id:" + entry.getInodeFile().getId());
      String addSql = addInodeFileFromEntry(entry.getInodeFile());
      return Collections.singletonList(addSql);
    } else if (entry.hasInodeLastModificationTime()) {
      LOG.trace("entry type:" + entry.getInodeLastModificationTime().getClass() +
          ", id:" + entry.getInodeLastModificationTime().getId());
      InodeLastModificationTimeEntry modTimeEntry = entry.getInodeLastModificationTime();
      String path = getPathByFileId(modTimeEntry.getId());
      FileDiff fileDiff = null;
      if (inBackup(path)) {
        fileDiff = new FileDiff(FileDiffType.METADATA);
        fileDiff.setSrc(path);
      }
      if (fileDiff != null) {
        fileDiff.getParameters().put("-mtime", "" + modTimeEntry.getLastModificationTimeMs());
        metaStore.insertFileDiff(fileDiff);
      }
      String modifySql = String.format(
          "UPDATE file SET modification_time = %s WHERE fid = '%s';",
          modTimeEntry.getLastModificationTimeMs(),
          modTimeEntry.getId());
      return Collections.singletonList(modifySql);
    } else if (entry.hasPersistDirectory()) {
      LOG.trace("entry type:" + entry.getPersistDirectory().getClass() +
          ", id:" + entry.getPersistDirectory().getId());
      PersistDirectoryEntry typedEntry = entry.getPersistDirectory();
      LOG.debug("Persist directory id " + typedEntry.getId());
      return Collections.singletonList("");
    } else if (entry.hasSetAttribute()) {
      LOG.trace("entry type:" + entry.getSetAttribute().getClass() +
          ", id:" + entry.getSetAttribute().getId());
      String setAttrSql = setAttributeFromEntry(entry.getSetAttribute());
      return Collections.singletonList(setAttrSql);
    } else if (entry.hasRename()) {
      LOG.trace("entry type:" + entry.getRename().getClass() +
          ", id:" + entry.getRename().getId());
      return renameFromEntry(entry.getRename());
    } else if (entry.hasDeleteFile()) {
      LOG.trace("entry type:" + entry.getDeleteFile().getClass() +
          ", id:" + entry.getDeleteFile().getId());
      String delSql = deleteFromEntry(entry.getDeleteFile());
      return Collections.singletonList(delSql);
    } else if (entry.hasAddMountPoint()) {
      LOG.trace("entry type:" + entry.getAddMountPoint().getClass() +
          ", alluxio path:" + entry.getAddMountPoint().getAlluxioPath() + 
          ", ufs path:" + entry.getAddMountPoint().getUfsPath());
      return Collections.singletonList(mountFromEntry(entry.getAddMountPoint()));
    } else if (entry.hasDeleteMountPoint()) {
      LOG.trace("entry type:" + entry.getDeleteMountPoint().getClass() +
          ", alluxio path:" + entry.getDeleteMountPoint().getAlluxioPath());
      return Collections.singletonList(unmountFromEntry(entry.getDeleteMountPoint()));
    } else if (entry.hasAsyncPersistRequest() 
        || entry.hasCompleteFile()
        || entry.hasInodeDirectoryIdGenerator()
        || entry.hasReinitializeFile()) {
      //Do nothing
    } else {
      throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(entry));
    }
    return Collections.emptyList();
  }
  
  private String addInodeFileFromEntry(InodeFileEntry inodeFileEntry) throws MetaStoreException {
    String inodePath = getPathFromInodeFile(inodeFileEntry);
    URIStatus status = null;
    try {
      status = fs.getStatus(new AlluxioURI(inodePath));
    } catch (IOException | AlluxioException e) {
      e.printStackTrace();
    } 
    FileInfo fileInfo = AlluxioUtil.convertFileStatus(status);
    if (inBackup(fileInfo.getPath())) {
      FileDiff fileDiff = new FileDiff(FileDiffType.APPEND);
      fileDiff.setSrc(fileInfo.getPath());
      fileDiff.getParameters().put("-offset", String.valueOf(0));
      // Note that "-length 0" means create an empty file
      fileDiff.getParameters()
          .put("-length", String.valueOf(fileInfo.getLength()));
      //add modification_time and access_time to filediff
      fileDiff.getParameters().put("-mtime", "" + fileInfo.getModificationTime());
      //add owner to filediff
      fileDiff.getParameters().put("-owner", "" + fileInfo.getOwner());
      fileDiff.getParameters().put("-group", "" + fileInfo.getGroup());
      //add Permission to filediff
      fileDiff.getParameters().put("-permission", "" + fileInfo.getPermission());
      metaStore.insertFileDiff(fileDiff);
    }
    metaStore.insertFile(fileInfo);
    return "";
  }
  
  private String setAttributeFromEntry(SetAttributeEntry setAttrEntry) throws MetaStoreException {
    String path = getPathByFileId(setAttrEntry.getId());
    FileDiff fileDiff = null;
    if (inBackup(path)) {
      fileDiff = new FileDiff(FileDiffType.METADATA);
      fileDiff.setSrc(path);
    }
    if (setAttrEntry.hasPinned()) {
      LOG.debug(String.format("File %s is pinned %s", setAttrEntry.getId(), setAttrEntry.getPinned()));
      //Todo
    } else if (setAttrEntry.hasTtl()) {
      LOG.debug(String.format("File %s has ttl %s with ttlAction %s", setAttrEntry.getId(), setAttrEntry.getTtl(), setAttrEntry.getTtlAction()));
      //Todo
    } else if (setAttrEntry.hasPersisted()) {
      LOG.debug(String.format("File %s is persisted %s", setAttrEntry.getId(), setAttrEntry.getPersisted()));
      //Todo
    } else if (setAttrEntry.hasOwner()) {
      if (fileDiff != null) {
        fileDiff.getParameters().put("-owner", "" + setAttrEntry.getOwner());
        metaStore.insertFileDiff(fileDiff);
      }
      //Todo
    } else if (setAttrEntry.hasGroup()) {
      if (fileDiff != null) {
        fileDiff.getParameters().put("-group", "" + setAttrEntry.getGroup());
        metaStore.insertFileDiff(fileDiff);
      }
      //Todo
    } else if (setAttrEntry.hasPermission()) {
      if (fileDiff != null) {
        fileDiff.getParameters().put("-permission", "" + (short)setAttrEntry.getPermission());
        metaStore.insertFileDiff(fileDiff);
      }
      return String.format(
          "UPDATE file SET permission = %s WHERE path = '%s';",
          (short)setAttrEntry.getPermission(), path);
    }
    return "";
  }
  
  private List<String> renameFromEntry(RenameEntry renameEntry) throws MetaStoreException {
    List<String> sqlist = new ArrayList<>();
    URIStatus dStatus = null;
    try {
      dStatus = fs.getStatus(new AlluxioURI(renameEntry.getDstPath()));
    } catch (IOException | AlluxioException e) {
      e.printStackTrace();
    }
    if (dStatus == null) {
      LOG.debug("Get rename dest status failed, {}", renameEntry.getDstPath());
    }
    FileInfo fileInfo = metaStore.getFile(renameEntry.getId());
    if (fileInfo == null) {
      if (dStatus != null) {
        fileInfo = AlluxioUtil.convertFileStatus(dStatus);
        metaStore.insertFile(fileInfo);
      }
    } else {
      FileDiff fileDiff = new FileDiff(FileDiffType.RENAME);
      String srcPath = fileInfo.getPath();
      if (inBackup(srcPath)) {
        fileDiff.setSrc(srcPath);
        fileDiff.getParameters().put("-dest", renameEntry.getDstPath());
        metaStore.insertFileDiff(fileDiff);
      }
      sqlist.add(String.format("UPDATE file SET path = replace(path, '%s', '%s') WHERE path = '%s';",
          srcPath, renameEntry.getDstPath(), srcPath));
      if (fileInfo.isdir()) {
        sqlist.add(String.format("UPDATE file SET path = replace(path, '%s', '%s') WHERE path LIKE '%s/%%';",
            srcPath, renameEntry.getDstPath(), srcPath));
      }
    }
    
    return sqlist;
  }

  private String deleteFromEntry(DeleteFileEntry deleteFileEntry) throws MetaStoreException {
    String path = getPathByFileId(deleteFileEntry.getId());
    if (inBackup(path)) {
      FileDiff fileDiff = new FileDiff(FileDiffType.DELETE);
      fileDiff.setSrc(path);
      metaStore.insertFileDiff(fileDiff);
    }
    return String.format("DELETE FROM file WHERE fid =%s;", deleteFileEntry.getId());
  }

  private String mountFromEntry(AddMountPointEntry mountPointEntry) {
    LOG.debug("Add mount alluxio path %s to ufs path %s", 
        mountPointEntry.getAlluxioPath(), mountPointEntry.getUfsPath());
    return "";
  }
  
  private String unmountFromEntry(DeleteMountPointEntry unmountEntry) {
    LOG.debug("Delete mount alluxio path %s", unmountEntry.getAlluxioPath());
    return "";
  }

  private boolean inBackup(String src) throws MetaStoreException {
    return metaStore.srcInbackup(src);
  }
  
  public String getPathFromInodeFile(InodeFileEntry fileEntry) throws MetaStoreException {
    long pid = fileEntry.getParentId();
    String fName = fileEntry.getName();
    FileInfo fileInfo = metaStore.getFile(pid);
    String pPath = "";
    if (fileInfo != null) {
      pPath = formatPath(fileInfo.getPath());
    }
    return pPath.concat(fName);
  }

  public String getPathFromInodeDir(InodeDirectoryEntry dirEntry) throws MetaStoreException {
    long pid = dirEntry.getParentId();
    String dName = dirEntry.getName();
    FileInfo fileInfo = metaStore.getFile(pid);
    String pDir = "";
    if (fileInfo != null) {
      pDir = formatPath(fileInfo.getPath());
    }
    return pDir.concat(dName);
  }

  public String getPathByFileId(long fid) throws MetaStoreException {
    FileInfo fileInfo = metaStore.getFile(fid);
    String path = "";
    if (fileInfo != null) {
      path = fileInfo.getPath();
    }
    return path;
  }
  
  private String formatPath(String path) {
    if (!path.endsWith(AlluxioURI.SEPARATOR)) {
      path += AlluxioURI.SEPARATOR;
    }
    return path;
  }
}
