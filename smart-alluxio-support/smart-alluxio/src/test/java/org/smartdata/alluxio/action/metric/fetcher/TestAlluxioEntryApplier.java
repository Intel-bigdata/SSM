package org.smartdata.alluxio.action.metric.fetcher;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.proto.journal.File.*;
import alluxio.proto.journal.Journal.JournalEntry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.smartdata.alluxio.metric.fetcher.AlluxioEntryApplier;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.TestDaoUtil;
import org.smartdata.model.BackUpInfo;
import org.smartdata.model.FileDiff;
import org.smartdata.model.FileDiffType;
import org.smartdata.model.FileInfo;

import java.util.List;

public class TestAlluxioEntryApplier extends TestDaoUtil {

  private MetaStore metaStore;

  @Before
  public void setUp() throws Exception {
    initDao();
    metaStore = new MetaStore(druidPool);
  }

  @After
  public void tearDown() throws Exception {
    closeDao();
  }

  @Test
  public void testInodeDirectoryApplier() throws Exception {
    FileSystem fs = Mockito.mock(FileSystem.class);
    AlluxioEntryApplier entryApplier = new AlluxioEntryApplier(metaStore, fs);

    FileInfo rootDir = FileInfo.newBuilder()
        .setFileId(0)
        .setIsdir(true)
        .setPath("/")
        .build();
    metaStore.insertFile(rootDir);

    alluxio.wire.FileInfo info1 = new alluxio.wire.FileInfo()
        .setFileId(1)
        .setPath("/dir1")
        .setLength(0L)
        .setFolder(true)
        .setBlockSizeBytes(1000000)
        .setLastModificationTimeMs(1528876616216L)
        .setCreationTimeMs(1528876616216L)
        .setMode(493)
        .setOwner("user1")
        .setGroup("group1");
    URIStatus status1 = new URIStatus(info1);
    Mockito.when(fs.getStatus(new AlluxioURI("/dir1"))).thenReturn(status1);

    InodeDirectoryEntry inodeDirectoryEntry = InodeDirectoryEntry.newBuilder()
        .setId(1)
        .setParentId(0)
        .setName("dir1")
        .setPersistenceState("NOT_PERSISTED")
        .setPinned(false)
        .setCreationTimeMs(1528876616216L)
        .setLastModificationTimeMs(1528876616216L)
        .setOwner("user1")
        .setGroup("group1")
        .setMode(493)
        .setMountPoint(false)
        .setDirectChildrenLoaded(false)
        .setTtl(-1L)
        .setTtlAction(PTtlAction.DELETE)
        .build();
    JournalEntry inodeDirectoryJEntry = JournalEntry.newBuilder()
        .setInodeDirectory(inodeDirectoryEntry)
        .build();
    entryApplier.apply(inodeDirectoryJEntry);

    Assert.assertTrue(metaStore.getFile().get(0).getPath().equals("/"));
    Assert.assertTrue(metaStore.getFile().get(1).getPath().equals("/dir1"));

    Assert.assertEquals("user1", metaStore.getFile("/dir1").getOwner());
    Assert.assertEquals(1528876616216L, metaStore.getFile("/dir1").getModificationTime());
  }

  @Test
  public void testInodeFileApplier() throws Exception {
    FileSystem fs = Mockito.mock(FileSystem.class);
    AlluxioEntryApplier entryApplier = new AlluxioEntryApplier(metaStore, fs);

    FileInfo fooDir = FileInfo.newBuilder()
        .setFileId(6)
        .setIsdir(true)
        .setPath("/foo")
        .build();
    metaStore.insertFile(fooDir);

    BackUpInfo backUpInfo = new BackUpInfo(1L, "/foo/foobar1", "remote/dest/", 10);
    metaStore.insertBackUpInfo(backUpInfo);

    alluxio.wire.FileInfo info1 = new alluxio.wire.FileInfo()
        .setFileId(33554431)
        .setPath("/foo/foobar1")
        .setLength(10L)
        .setFolder(false)
        .setBlockSizeBytes(536870912)
        .setLastModificationTimeMs(1515665470681L)
        .setCreationTimeMs(1515665470681L)
        .setMode(420)
        .setOwner("user1")
        .setGroup("group1");
    URIStatus status1 = new URIStatus(info1);
    Mockito.when(fs.getStatus(new AlluxioURI("/foo/foobar1"))).thenReturn(status1);

    InodeFileEntry inodeFileEntry = InodeFileEntry.newBuilder()
        .setId(33554431)
        .setParentId(6)
        .setName("foobar1")
        .setPersistenceState("NOT_PERSISTED")
        .setPinned(false)
        .setCreationTimeMs(1515665470681L)
        .setBlockSizeBytes(536870912)
        .setLength(10L)
        .setCompleted(false)
        .setCacheable(true)
        .setTtl(-1L)
        .setOwner("user1")
        .setGroup("group1")
        .setMode(420)
        .setTtlAction(PTtlAction.DELETE)
        .build();
    JournalEntry inodeFileJEntry = JournalEntry.newBuilder()
        .setInodeFile(inodeFileEntry)
        .build();
    entryApplier.apply(inodeFileJEntry);

    Assert.assertEquals(33554431, metaStore.getFile("/foo/foobar1").getFileId());
    Assert.assertEquals("user1", metaStore.getFile("/foo/foobar1").getOwner());
    Assert.assertEquals(536870912, metaStore.getFile("/foo/foobar1").getBlocksize());

    List<FileDiff> fileDiffs = metaStore.getFileDiffsByFileName("/foo/foobar1");

    Assert.assertTrue(fileDiffs.size() > 0);

    for (FileDiff fileDiff : fileDiffs) {
      if (fileDiff.getDiffType().equals(FileDiffType.APPEND)) {
        Assert.assertTrue(fileDiff.getParameters().get("-owner").equals("user1"));
        Assert.assertTrue(fileDiff.getParameters().get("-mtime").equals("1515665470681"));
        Assert.assertTrue(fileDiff.getParameters().get("-length").equals("10"));
      }
    }
  }

  @Test
  public void testInodeLastMTimeApplier() throws Exception {
    FileSystem fs = Mockito.mock(FileSystem.class);
    AlluxioEntryApplier entryApplier = new AlluxioEntryApplier(metaStore, fs);

    FileInfo fooFile = FileInfo.newBuilder()
        .setFileId(5)
        .setIsdir(true)
        .setPath("/baz")
        .build();
    metaStore.insertFile(fooFile);

    BackUpInfo backUpInfo = new BackUpInfo(1L, "/baz", "remote/dest/", 10);
    metaStore.insertBackUpInfo(backUpInfo);

    alluxio.wire.FileInfo info1 = new alluxio.wire.FileInfo()
        .setFileId(5)
        .setPath("/baz")
        .setLength(0L)
        .setFolder(true)
        .setBlockSizeBytes(1000000)
        .setLastModificationTimeMs(1515665470681L)
        .setCreationTimeMs(1515665470681L)
        .setMode(493)
        .setOwner("user1")
        .setGroup("group1");
    URIStatus status1 = new URIStatus(info1);
    Mockito.when(fs.getStatus(new AlluxioURI("/baz"))).thenReturn(status1);

    InodeLastModificationTimeEntry inodeLastMTimeEntry = InodeLastModificationTimeEntry.newBuilder()
        .setId(5)
        .setLastModificationTimeMs(1515667810911L)
        .build();

    JournalEntry inodeLastMTimeJEntry = JournalEntry.newBuilder()
        .setInodeLastModificationTime(inodeLastMTimeEntry)
        .build();
    entryApplier.apply(inodeLastMTimeJEntry);

    List<FileDiff> fileDiffs = metaStore.getFileDiffsByFileName("/baz");

    Assert.assertTrue(fileDiffs.size() > 0);

    for (FileDiff fileDiff : fileDiffs) {
      if (fileDiff.getDiffType().equals(FileDiffType.METADATA)) {
        Assert.assertEquals("/baz", fileDiff.getSrc());
        Assert.assertEquals("1515667810911", fileDiff.getParameters().get("-mtime"));
      }
    }
  }

  @Test
  public void testSetAttributeApplier() throws Exception {
    FileSystem fs = Mockito.mock(FileSystem.class);
    AlluxioEntryApplier entryApplier = new AlluxioEntryApplier(metaStore, fs);

    FileInfo fooFile = FileInfo.newBuilder()
        .setFileId(33554431)
        .setIsdir(false)
        .setPath("/foo/foobar")
        .build();
    metaStore.insertFile(fooFile);

    BackUpInfo backUpInfo = new BackUpInfo(1L, "/foo/foobar", "remote/dest/", 10);
    metaStore.insertBackUpInfo(backUpInfo);

    alluxio.wire.FileInfo info1 = new alluxio.wire.FileInfo()
        .setFileId(33554431)
        .setPath("/foo/foobar")
        .setLength(100L)
        .setFolder(false)
        .setBlockSizeBytes(210000)
        .setLastModificationTimeMs(1515665470681L)
        .setCreationTimeMs(1515665470681L)
        .setMode(493)
        .setOwner("user1")
        .setGroup("group1");
    URIStatus status1 = new URIStatus(info1);
    Mockito.when(fs.getStatus(new AlluxioURI("/foo/foobar"))).thenReturn(status1);

    SetAttributeEntry setAttributeEntry = SetAttributeEntry.newBuilder()
        .setId(33554431)
        .setOpTimeMs(1515667208590658L)
        .setPermission(511)
        .build();

    JournalEntry setAttributeJEntry = JournalEntry.newBuilder()
        .setSetAttribute(setAttributeEntry)
        .build();
    entryApplier.apply(setAttributeJEntry);

    List<FileDiff> fileDiffs = metaStore.getFileDiffsByFileName("/foo/foobar");

    Assert.assertTrue(fileDiffs.size() > 0);

    for (FileDiff fileDiff : fileDiffs) {
      if (fileDiff.getDiffType().equals(FileDiffType.METADATA)) {
        Assert.assertEquals("511", fileDiff.getParameters().get("-permission"));
      }
    }
  }

  @Test
  public void testRenameApplier() throws Exception {
    FileSystem fs = Mockito.mock(FileSystem.class);
    AlluxioEntryApplier entryApplier = new AlluxioEntryApplier(metaStore, fs);

    FileInfo fooFile = FileInfo.newBuilder()
        .setFileId(50331647)
        .setIsdir(false)
        .setPath("/bar/foobar1")
        .build();
    metaStore.insertFile(fooFile);

    BackUpInfo backUpInfo = new BackUpInfo(1L, "/bar/foobar1", "remote/dest/", 10);
    metaStore.insertBackUpInfo(backUpInfo);

    alluxio.wire.FileInfo info1 = new alluxio.wire.FileInfo()
        .setFileId(50331647)
        .setPath("/bar/foobar1")
        .setLength(300L)
        .setFolder(false)
        .setBlockSizeBytes(310000)
        .setLastModificationTimeMs(1515665270681L)
        .setCreationTimeMs(1515665270681L)
        .setMode(493)
        .setOwner("user1")
        .setGroup("group1");
    URIStatus status1 = new URIStatus(info1);
    Mockito.when(fs.getStatus(new AlluxioURI("/bar/foobar1"))).thenReturn(status1);

    RenameEntry renameEntry = RenameEntry.newBuilder()
        .setId(50331647)
        .setOpTimeMs(1515666148444L)
        .setDstPath("/bar/foobar1_new")
        .build();

    JournalEntry renameJEntry = JournalEntry.newBuilder()
        .setRename(renameEntry)
        .build();
    entryApplier.apply(renameJEntry);

    List<FileDiff> fileDiffs = metaStore.getFileDiffsByFileName("/bar/foobar1");

    Assert.assertTrue(fileDiffs.size() > 0);

    for (FileDiff fileDiff : fileDiffs) {
      if (fileDiff.getDiffType().equals(FileDiffType.RENAME)) {
        Assert.assertEquals("/bar/foobar1", fileDiff.getSrc());
        Assert.assertEquals("/bar/foobar1_new", fileDiff.getParameters().get("-dest"));
      }
    }
  }

  @Test
  public void testDeleteFileApplier() throws Exception {
    FileSystem fs = Mockito.mock(FileSystem.class);
    AlluxioEntryApplier entryApplier = new AlluxioEntryApplier(metaStore, fs);

    FileInfo fooFile = FileInfo.newBuilder()
        .setFileId(100663295)
        .setIsdir(false)
        .setPath("/foo/foobar_del")
        .build();
    metaStore.insertFile(fooFile);

    BackUpInfo backUpInfo = new BackUpInfo(1L, "/foo/foobar_del", "remote/dest/", 10);
    metaStore.insertBackUpInfo(backUpInfo);

    alluxio.wire.FileInfo info1 = new alluxio.wire.FileInfo()
        .setFileId(100663295)
        .setPath("/foo/foobar_del")
        .setLength(500L)
        .setFolder(false)
        .setBlockSizeBytes(510000)
        .setLastModificationTimeMs(1515665270681L)
        .setCreationTimeMs(1515665270681L)
        .setMode(493)
        .setOwner("user1")
        .setGroup("group1");
    URIStatus status1 = new URIStatus(info1);
    Mockito.when(fs.getStatus(new AlluxioURI("/foo/foobar_del"))).thenReturn(status1);

    DeleteFileEntry deleteFileEntry = DeleteFileEntry.newBuilder()
        .setId(100663295)
        .setOpTimeMs(1515737580798L)
        .setAlluxioOnly(true)
        .setRecursive(false)
        .build();

    JournalEntry deleteFileJEntry = JournalEntry.newBuilder()
        .setDeleteFile(deleteFileEntry)
        .build();
    entryApplier.apply(deleteFileJEntry);

    List<FileDiff> fileDiffs = metaStore.getFileDiffsByFileName("/foo/foobar_del");

    Assert.assertTrue(fileDiffs.size() > 0);

    for (FileDiff fileDiff : fileDiffs) {
      if (fileDiff.getDiffType().equals(FileDiffType.DELETE)) {
        Assert.assertEquals("/foo/foobar_del", fileDiff.getSrc());
      }
    }
  }

}
