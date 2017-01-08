package org.apache.hadoop.ssm;

import org.apache.hadoop.hdfs.protocol.FilesAccessInfo;
import org.apache.hadoop.hdfs.protocol.NNEvent;
import org.apache.hadoop.ssm.api.Expression.FileFilterRule;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hdfs.protocol.NNEvent.EV_DELETE;
import static org.apache.hadoop.hdfs.protocol.NNEvent.EV_RENAME;

/**
 * Created by root on 11/4/16.
 */
public class FileAccessMap {
  private HashMap<String, FileAccess> fileMap;

  public FileAccessMap() {
    fileMap = new HashMap<String, FileAccess>();
  }

  public HashMap<String, FileAccess> getMap() {return fileMap;}

  public FileAccess get(String fileName) {
    return fileMap.get(fileName);
  }

  public FileAccess put(String fileName, FileAccess fileAccess) {
    return fileMap.put(fileName, fileAccess);
  }

  public FileAccess remove(String fileName) {
    return fileMap.remove(fileName);
  }

  public boolean containsKey(String fileName) {
    return fileMap.containsKey(fileName);
  }

  public Set<Map.Entry<String, FileAccess>> entrySet() { return fileMap.entrySet();}

  public void rename(String srcName, String dstName, FileFilterRule fileFilterRule){
    FileAccess srcFileAccess = fileMap.get(srcName);
    FileAccess dstFileAccess = fileMap.get(dstName);
    if (srcFileAccess != null) { // src must be a file
      // update fileMap
      fileMap.remove(srcName);
      FileAccess renamedFileAccess = new FileAccess(srcFileAccess);
      renamedFileAccess.setFileName(dstName);
      if (dstFileAccess != null) {
        renamedFileAccess.increaseAccessCount(dstFileAccess.getAccessCount());
      }
      if (fileFilterRule == null || fileFilterRule.meetCondition(dstName)) {
        fileMap.put(dstName, renamedFileAccess);
      }
    }
    else { // search all files under this directory
      HashMap<String, FileAccess> renameMap = new HashMap<String, FileAccess>();
      for (Iterator<Map.Entry<String, FileAccess>> it = fileMap.entrySet().iterator(); it.hasNext(); ) {
        Map.Entry<String, FileAccess> entry = it.next();
        String fileName = entry.getKey();
        FileAccess fileAccess = entry.getValue();
        if (fileName.startsWith(srcName) &&
                (srcName.charAt(srcName.length() - 1) == '/' || fileName.charAt(srcName.length()) == '/')) {
          // update fileMap
          String newFileName = dstName + fileName.substring(srcName.length());
          fileAccess.setFileName(newFileName);
          dstFileAccess = fileMap.get(fileAccess.getFileName());
          if (dstFileAccess != null) {
            fileAccess.increaseAccessCount(dstFileAccess.getAccessCount());
          }
          it.remove();
          if (fileFilterRule == null || fileFilterRule.meetCondition(dstName)) {
            renameMap.put(fileAccess.getFileName(), fileAccess);
          }
        }
      }
      fileMap.putAll(renameMap);
    }
  }

  public void delete(String name) {
    if (fileMap.containsKey(name)) {
      fileMap.remove(name);
    }
    else {
      for (Iterator<Map.Entry<String, FileAccess>> it = fileMap.entrySet().iterator(); it.hasNext(); ) {
        Map.Entry<String, FileAccess> entry = it.next();
        String fileName = entry.getKey();
        if (fileName.startsWith(name) &&
                (name.charAt(name.length() - 1) == '/' || fileName.charAt(name.length()) == '/')) {
          it.remove();
        }
      }
    }
  }

  /**
   * Process rename and delete events
   * @param filesAccessInfo
   */
  public void processNnEvents(FilesAccessInfo filesAccessInfo) {
    if (fileMap == null) {
      return;
    }
    if (filesAccessInfo.getNnEvents() != null) {
      for (NNEvent nnEvent : filesAccessInfo.getNnEvents()) {
        switch (nnEvent.getEventType()) {
          case EV_RENAME:
            rename(nnEvent.getArgs()[0], nnEvent.getArgs()[1], null);
            break;
          case EV_DELETE:
            delete(nnEvent.getArgs()[0]);
            break;
          default:
        }
      }
    }
  }

  /**
   * Process rename and delete events with FileFilterRule to decide
   * whether or not to remove the file according to FileFilterRule after rename
   * @param filesAccessInfo
   * @param fileFilterRule
   */
  public void processNnEvents(FilesAccessInfo filesAccessInfo, FileFilterRule fileFilterRule) {
    if (fileMap == null) {
      return;
    }
    if (filesAccessInfo.getNnEvents() != null) {
      for (NNEvent nnEvent : filesAccessInfo.getNnEvents()) {
        switch (nnEvent.getEventType()) {
          case EV_RENAME:
            rename(nnEvent.getArgs()[0], nnEvent.getArgs()[1], fileFilterRule);
            break;
          case EV_DELETE:
            delete(nnEvent.getArgs()[0]);
            break;
          default:
        }
      }
    }
  }

  /**
   * Update fileMap
   * @param filesAccessInfo
   */
  public void updateFileMap(FilesAccessInfo filesAccessInfo) {
    if (filesAccessInfo.getFilesAccessed() != null) {
      for (int i = 0; i < filesAccessInfo.getFilesAccessed().size(); i++) {
        String fileName = filesAccessInfo.getFilesAccessed().get(i);
        Integer fileAccessCount = filesAccessInfo.getFilesAccessCounts().get(i);
        FileAccess fileAccess = get(fileName);
        if (fileAccess != null) {
          fileAccess.increaseAccessCount(fileAccessCount);
        } else {
          fileAccess = new FileAccess(fileName, fileAccessCount);
          put(fileName, fileAccess);
        }
      }
    }
  }

  /**
   * Update fileMap with FileFilterRule to only store the files which accord to FileFilterRule
   * @param filesAccessInfo
   */
  public void updateFileMap(FilesAccessInfo filesAccessInfo, FileFilterRule fileFilterRule) {
    if (filesAccessInfo.getFilesAccessed() != null) {
      for (int i = 0; i < filesAccessInfo.getFilesAccessed().size(); i++) {
        String fileName = filesAccessInfo.getFilesAccessed().get(i);
        Integer fileAccessCount = filesAccessInfo.getFilesAccessCounts().get(i);
        if (fileFilterRule.meetCondition(fileName)) {
          FileAccess fileAccess = get(fileName);
          if (fileAccess != null) {
            fileAccess.increaseAccessCount(fileAccessCount);
          } else {
            fileAccess = new FileAccess(fileName, fileAccessCount);
            put(fileName, fileAccess);
          }
        }
      }
    }
  }

}
