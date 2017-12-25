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
package org.smartdata.model;


import java.util.Objects;

/**
 * FileState.
 */
public class FileState {
  protected String path;
  protected FileType fileType;
  protected FileStage fileStage;

  public FileState(String path, FileType fileType, FileStage fileStage) {
    this.path = path;
    this.fileType = fileType;
    this.fileStage = fileStage;
  }

  public String getPath() {
    return path;
  }

  public FileType getFileType() {
    return fileType;
  }

  public FileStage getFileStage() {
    return fileStage;
  }

  public void setFileStage(FileStage fileStage) {
    this.fileStage = fileStage;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileState fileState = (FileState) o;
    return path.equals(fileState.getPath())
        && fileType.equals(fileState.getFileType())
        && fileStage.equals(fileState.getFileStage());
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, fileType, fileStage);
  }

  /**
   * FileType indicates type of a file.
   */
  public enum FileType {
    NORMAL(0),
    COMPACT(1),
    COMPRESSION(2),
    S3(3);

    private final int value;

    FileType(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    public static FileType fromValue(int value) {
      for (FileType t : values()) {
        if (t.getValue() == value) {
          return t;
        }
      }
      return null;
    }

    public static FileType fromName(String name) {
      for (FileType t : values()) {
        if (t.toString().equalsIgnoreCase(name)) {
          return t;
        }
      }
      return null;
    }
  }

  public enum FileStage {
    PROCESSING(0),
    DONE(1);

    private final int value;

    FileStage(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    public static FileStage fromValue(int value) {
      for (FileStage t : values()) {
        if (t.getValue() == value) {
          return t;
        }
      }
      return null;
    }

    public static FileStage fromName(String name) {
      for (FileStage t : values()) {
        if (t.toString().equalsIgnoreCase(name)) {
          return t;
        }
      }
      return null;
    }
  }
}
