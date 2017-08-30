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
package org.smartdata.alluxio.filesystem;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.client.SmartClient;
import org.smartdata.metrics.FileAccessEvent;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.ExistsOptions;
import alluxio.client.file.options.FreeOptions;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.LoadMetadataOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.options.RenameOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.client.file.options.UnmountOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;

// SmartAlluxioFileSystem usage:
// FileSystem fs = new SmartAlluxioFileSystem();
// InputStream is = fs.openFile(new AlluxioURI("/path1/file1"));
// ...
public class SmartAlluxioFileSystem implements FileSystem {
  protected static final Logger LOG = LoggerFactory.getLogger(SmartAlluxioFileSystem.class);

  FileSystem rawFs;
  SmartClient smartClient;
  Configuration conf;

  public SmartAlluxioFileSystem(FileSystemContext context) throws IOException {
    rawFs = FileSystem.Factory.get(context);
    conf = new Configuration();
    smartClient = new SmartClient(conf);
  }

  public SmartAlluxioFileSystem() throws IOException {
    rawFs = FileSystem.Factory.get();
    conf = new Configuration();
    smartClient = new SmartClient(conf);
  }

  @Override
  public void createDirectory(AlluxioURI uri)
      throws FileAlreadyExistsException, InvalidPathException, IOException,
      AlluxioException {
    rawFs.createDirectory(uri);
  }

  @Override
  public void createDirectory(AlluxioURI uri, CreateDirectoryOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException,
      AlluxioException {
    rawFs.createDirectory(uri, options);
  }

  @Override
  public FileOutStream createFile(AlluxioURI uri)
      throws FileAlreadyExistsException, InvalidPathException, IOException,
      AlluxioException {
    return rawFs.createFile(uri);
  }

  @Override
  public FileOutStream createFile(AlluxioURI uri, CreateFileOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException,
      AlluxioException {
    return rawFs.createFile(uri, options);
  }

  @Override
  public void delete(AlluxioURI uri) throws DirectoryNotEmptyException,
      FileDoesNotExistException, IOException, AlluxioException {
    rawFs.delete(uri);
  }

  @Override
  public void delete(AlluxioURI uri, DeleteOptions options)
      throws DirectoryNotEmptyException, FileDoesNotExistException,
      IOException, AlluxioException {
    rawFs.delete(uri, options);
  }

  @Override
  public boolean exists(AlluxioURI uri) throws InvalidPathException,
      IOException, AlluxioException {
    return rawFs.exists(uri);
  }

  @Override
  public boolean exists(AlluxioURI uri, ExistsOptions options)
      throws InvalidPathException, IOException, AlluxioException {
    return rawFs.exists(uri, options);
  }

  @Override
  public void free(AlluxioURI uri) throws FileDoesNotExistException,
      IOException, AlluxioException {
    rawFs.free(uri);
  }

  @Override
  public void free(AlluxioURI uri, FreeOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    rawFs.free(uri, options);
  }

  @Override
  public URIStatus getStatus(AlluxioURI uri) throws FileDoesNotExistException,
      IOException, AlluxioException {
    return rawFs.getStatus(uri);
  }

  @Override
  public URIStatus getStatus(AlluxioURI uri, GetStatusOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    return rawFs.getStatus(uri, options);
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI uri)
      throws FileDoesNotExistException, IOException, AlluxioException {
    return rawFs.listStatus(uri);
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI uri, ListStatusOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    return rawFs.listStatus(uri, options);
  }

  @Override
  public void loadMetadata(AlluxioURI uri) throws FileDoesNotExistException,
      IOException, AlluxioException {
    rawFs.loadMetadata(uri);
  }

  @Override
  public void loadMetadata(AlluxioURI uri, LoadMetadataOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    rawFs.loadMetadata(uri, options);
  }

  @Override
  public void mount(AlluxioURI uri1, AlluxioURI uri2) throws IOException,
      AlluxioException {
    rawFs.mount(uri1, uri2);
  }

  @Override
  public void mount(AlluxioURI uri1, AlluxioURI uri2, MountOptions options)
      throws IOException, AlluxioException {
    rawFs.mount(uri1, uri2, options);
  }

  @Override
  public FileInStream openFile(AlluxioURI uri)
      throws FileDoesNotExistException, IOException, AlluxioException {
    reportFileAccessEvent(uri.getPath());
    return rawFs.openFile(uri);
  }

  @Override
  public FileInStream openFile(AlluxioURI uri, OpenFileOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    reportFileAccessEvent(uri.getPath());
    return rawFs.openFile(uri, options);
  }

  @Override
  public void rename(AlluxioURI uri1, AlluxioURI uri2)
      throws FileDoesNotExistException, IOException, AlluxioException {
    rawFs.rename(uri1, uri2);
  }

  @Override
  public void rename(AlluxioURI uri1, AlluxioURI uri2, RenameOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    rawFs.rename(uri1, uri2, options);
  }

  @Override
  public void setAttribute(AlluxioURI uri) throws FileDoesNotExistException,
      IOException, AlluxioException {
    rawFs.setAttribute(uri);
  }

  @Override
  public void setAttribute(AlluxioURI uri, SetAttributeOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    rawFs.setAttribute(uri, options);
  }

  @Override
  public void unmount(AlluxioURI uri) throws IOException, AlluxioException {
    rawFs.unmount(uri);
  }

  @Override
  public void unmount(AlluxioURI uri, UnmountOptions options)
      throws IOException, AlluxioException {
    rawFs.unmount(uri, options);
  }

  private void reportFileAccessEvent(String src) {
    try {
      smartClient.reportFileAccessEvent(new FileAccessEvent(src));
    } catch (IOException e) {
      LOG.error("Can not report file access event to SmartServer: " + src);
    }
  }
}
