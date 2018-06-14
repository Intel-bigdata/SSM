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
package alluxio.master.journal.ufs;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.master.NoopMaster;
import alluxio.master.journal.JournalReader;
import alluxio.proto.journal.Journal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * Util for reading the journal entries given a range of sequence numbers.
 */
public class AlluxioJournalUtil {

  public static final Logger LOG = LoggerFactory.getLogger(AlluxioJournalUtil.class);

  private static String sMaster = Constants.FILE_SYSTEM_MASTER_NAME;

  /**
   * @param conf smart configuration
   * @return the current entry sequence number
   */
  public static Long getCurrentSeqNum(SmartConf conf) {
    UfsJournal journal =
        new UfsJournalSystem(getJournalLocation(conf), 0).createJournal(new NoopMaster(sMaster));
    UfsJournalFile currentLog;
    try {
      currentLog = UfsJournalSnapshot.getCurrentLog(journal);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    long sn = -1L;
    if (currentLog != null) {
      try (JournalReader reader = new UfsJournalReader(journal, currentLog.getStart(), true)) {
        Journal.JournalEntry entry;
        while ((entry = reader.read()) != null) {
          sn = entry.getSequenceNumber();
          if (sn >= Long.MAX_VALUE) {
            break;
          }
        }
      } catch (Exception e) {
        LOG.error("Failed to read next journal entry.", e);
      }
    }
    return sn;
  }

  /**
   * @param conf smart configuration
   * @param startSn journal entry sequence number
   * @return journal reader
   */
  public static JournalReader getJournalReaderFromSn(SmartConf conf, Long startSn) {
    UfsJournal journal =
        new UfsJournalSystem(getJournalLocation(conf), 0).createJournal(new NoopMaster(sMaster));
    JournalReader reader = new UfsJournalReader(journal, startSn, true);
    return reader;
  }

  /**
   * @param conf smart configuration
   * @return the journal location
   */
  private static URI getJournalLocation(SmartConf conf) {
    String alluxioMasterJournalDir = conf.get(
        SmartConfKeys.SMART_ALLUXIO_MASTER_JOURNAL_DIR_KEY, "/opt/alluxio/journal");
    Configuration.set(PropertyKey.MASTER_JOURNAL_FOLDER, alluxioMasterJournalDir);
    String journalDirectory = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    if (!journalDirectory.endsWith(AlluxioURI.SEPARATOR)) {
      journalDirectory += AlluxioURI.SEPARATOR;
    }
    try {
      return new URI(journalDirectory);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

}