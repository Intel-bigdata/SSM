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

package org.smartdata.alluxio.action.metric.fetcher;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.options.*;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.journal.JournalUtils;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartConstants;
import org.smartdata.alluxio.metric.fetcher.AlluxioEntryApplier;
import org.smartdata.alluxio.metric.fetcher.AlluxioEntryFetcher;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.TestDaoUtil;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

public class TestAlluxioEntryFetcher extends TestDaoUtil {
  public static final Logger LOG = LoggerFactory.getLogger(TestAlluxioEntryFetcher.class);

  LocalAlluxioCluster localAlluxioCluster;
  FileSystem fs;
  MetaStore metaStore;

  @Rule
  public TemporaryFolder mTemporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    localAlluxioCluster = new LocalAlluxioCluster(2);
    localAlluxioCluster.initConfiguration();
    Configuration.set(PropertyKey.WEB_RESOURCES,
        PathUtils.concatPath(System.getProperty("user.dir"), "src/test/webapp"));
    localAlluxioCluster.start();
    fs = localAlluxioCluster.getClient();
    initDao();
    metaStore = new MetaStore(druidPool);
  }

  @After
  public void tearDown() throws Exception {
    if (localAlluxioCluster != null) {
      localAlluxioCluster.stop();
    }
    closeDao();
  }

  @Test
  public void testEntryFetcher() throws Exception {
    URI journalLocation = JournalUtils.getJournalLocation();
    SmartConf conf = new SmartConf();
    conf.set(SmartConfKeys.SMART_ALLUXIO_MASTER_JOURNAL_DIR_KEY, journalLocation.getPath());

    EntryApplierForTest entryApplierForTest = new EntryApplierForTest(metaStore, fs);
    final AlluxioEntryFetcher entryFetcher = new AlluxioEntryFetcher(fs, metaStore,
        Executors.newScheduledThreadPool(2), entryApplierForTest, new Callable() {
      @Override
      public Object call() throws Exception {
        return null; // Do nothing
      }
    }, conf);

    Assert.assertFalse(AlluxioEntryFetcher.canReadFromLastSeqNum(100L));

    /**
     * Generate such local structure
     *      ├── foo |
     *              ├── foobar1
     *              └── foobar2
     *      ├── bar |
     *              └── foobar3
     *      └── foobar4
     */
    fs.createDirectory(new AlluxioURI("/foo"));
    fs.createDirectory(new AlluxioURI("/bar"));

    FileSystemTestUtils.createByteFile(fs, "/foo/foobar1", WriteType.CACHE_THROUGH, 10);
    FileSystemTestUtils.createByteFile(fs, "/foo/foobar2", WriteType.CACHE_THROUGH, 20);
    FileSystemTestUtils.createByteFile(fs, "/bar/foobar3", WriteType.CACHE_THROUGH, 30);
    FileSystemTestUtils.createByteFile(fs, "/foobar4", WriteType.CACHE_THROUGH, 40);

    Thread thread = new Thread() {
      public void run() {
        try {
          entryFetcher.start();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    thread.start();

    // need wait long enough to finish namespace fetcher
    Thread.sleep(10*1000);

    fs.setAttribute(new AlluxioURI("/foo/foobar1"), SetAttributeOptions.defaults().setPersisted(true));
    fs.setAttribute(new AlluxioURI("/foo/foobar2"), SetAttributeOptions.defaults().setPinned(true));
    fs.setAttribute(new AlluxioURI("/bar/foobar3"), SetAttributeOptions.defaults().setTtl(1000000L));

    String mLocalUfsRoot = mTemporaryFolder.getRoot().getAbsolutePath();
    UnderFileSystem mLocalUfs = UnderFileSystem.Factory.create(mLocalUfsRoot);
    String mountpath = PathUtils.concatPath(mLocalUfsRoot, "mtd_ufs");
    mLocalUfs.mkdirs(mountpath);

    Assert.assertTrue(new File(mountpath).exists());

    fs.mount(new AlluxioURI("/mtd_t"), new AlluxioURI(mountpath), MountOptions.defaults());

    fs.rename(new AlluxioURI("/foo/foobar1"), new AlluxioURI("/foo/foo1"), RenameOptions.defaults());
    fs.delete(new AlluxioURI("/bar/foobar3"), DeleteOptions.defaults().setRecursive(true));

    fs.createDirectory(new AlluxioURI("/baz"));
    FileSystemTestUtils.createByteFile(fs, "/baz/foobar5", WriteType.CACHE_THROUGH, 50);

    Mode mode = new Mode((short)0755);
    fs.setAttribute(new AlluxioURI("/baz/foobar5"), SetAttributeOptions.defaults().setMode(mode));

    // free action does not generate journal entry
    fs.free(new AlluxioURI("/baz"), FreeOptions.defaults().setRecursive(true));

    while (entryApplierForTest.getEntries().size() != 16) {
      Thread.sleep(100);
    }

    List<JournalEntry> entries = entryApplierForTest.getEntries();
    Assert.assertTrue(entries.get(0).hasSetAttribute() && entries.get(0).getSetAttribute().hasPersisted());
    Assert.assertTrue(entries.get(1).hasSetAttribute() && entries.get(1).getSetAttribute().hasPinned());
    Assert.assertTrue(entries.get(2).hasSetAttribute() && entries.get(2).getSetAttribute().hasTtl());
    Assert.assertTrue(entries.get(3).hasInodeLastModificationTime());
    Assert.assertTrue(entries.get(4).hasInodeDirectoryIdGenerator());
    Assert.assertTrue(entries.get(5).hasInodeDirectory());
    Assert.assertTrue(entries.get(6).hasAddMountPoint());
    Assert.assertTrue(entries.get(7).hasRename());
    Assert.assertTrue(entries.get(8).hasDeleteFile());
    Assert.assertTrue(entries.get(9).hasInodeLastModificationTime());
    Assert.assertTrue(entries.get(10).hasInodeDirectoryIdGenerator());
    Assert.assertTrue(entries.get(11).hasInodeDirectory());
    Assert.assertTrue(entries.get(12).hasInodeLastModificationTime());
    Assert.assertTrue(entries.get(13).hasInodeFile());
    Assert.assertTrue(entries.get(14).hasCompleteFile());
    Assert.assertTrue(entries.get(15).hasSetAttribute() && entries.get(15).getSetAttribute().hasPermission());

    entryFetcher.stop();

    Assert.assertTrue(metaStore.containSystemInfo(SmartConstants.SMART_ALLUXIO_LAST_ENTRY_SN));
    Assert.assertTrue(AlluxioEntryFetcher.canReadFromLastSeqNum(
        Long.parseLong(metaStore.getSystemInfoByProperty(SmartConstants.SMART_ALLUXIO_LAST_ENTRY_SN).getValue())));
  }

  private static class EntryApplierForTest extends AlluxioEntryApplier {
    private List<JournalEntry> entries = new ArrayList<>();

    public EntryApplierForTest(MetaStore metaStore, FileSystem fs) {
      super(metaStore, fs);
    }

    @Override
    public void apply(JournalEntry entry) {
      entries.add(entry);
    }

    public List<JournalEntry> getEntries() {
      return entries;
    }
  }

}
