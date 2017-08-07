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
package org.smartdata.hdfs.action.move;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.hdfs.action.SchedulePlan;

import java.io.IOException;
import java.net.URI;

/**
 * HDFS move based move runner.
 */
public class MoverBasedMoveRunner extends MoveRunner {
  private static final Logger LOG = LoggerFactory.getLogger(MoverBasedMoveRunner.class);
  private Configuration conf;
  private MoverStatus actionStatus;

  static class Pair {
    URI ns;
    Path path;

    Pair(URI ns, Path path) {
      this.ns = ns;
      this.path = path;
    }
  }

  public MoverBasedMoveRunner(Configuration conf, MoverStatus actionStatus) {
    this.conf = conf;
    this.actionStatus = actionStatus;
  }

  @Override
  public void move(String file, SchedulePlan plan) throws Exception {
    if (plan == null) {
      move(file);
    } else {
      MoverExecutor executor = new MoverExecutor(conf, 10, 20);
      executor.executeMove(plan);
    }
  }

  @Override
  public void move(String file) throws Exception {
    // TODO: integrate
    throw new IOException("Method move(String file) not implemented.");
//    long startTime = Time.now();
//    NameNodeConnector nnc = null;
//    final long sleeptime =
//        conf.getLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
//            DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 2000 +
//            conf.getLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY,
//                DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT) * 1000;
//    try {
//      final Pair pathPair = getNameNodePathsToMove(file);
//      //return Mover.run(pathPair, conf, actionStatus);
//      LOG.info("Namenode = " + pathPair.ns);
//      nnc = new NameNodeConnector(pathPair.ns, conf);
//
//      //while (true) {
//        final Mover m = new Mover(nnc, pathPair.path, conf, actionStatus);
//        final ExitStatus r = m.run();
//
//        if (r == ExitStatus.SUCCESS) {
//          actionStatus.setMovedBlocks(actionStatus.getTotalBlocks());
//          IOUtils.cleanup(null, nnc);
//          LOG.info("Mover Successful: all blocks satisfy"
//              + " the specified storage policy. Exiting...");
//          return;
//        } else if (r != ExitStatus.IN_PROGRESS) {
//          if (r == ExitStatus.NO_MOVE_PROGRESS) {
//            LOG.error("Failed to move some blocks after "
//                + 1 + " retries. Exiting...");
//          } else if (r == ExitStatus.NO_MOVE_BLOCK) {
//            LOG.error("Some blocks can't be moved. Exiting...");
//          } else {
//            LOG.error("Mover failed. Exiting with status " + r + "... ");
//          }
//          return;
//        }
//        //Thread.sleep(sleeptime);
//      //}
//    } finally {
//      IOUtils.cleanup(null, nnc);
//      long runningTime = Time.now() - startTime;
//      Log.format("%-24s ", DateFormat.getDateTimeInstance().format(new Date()));
//      LOG.info("Mover took " + StringUtils.formatTime(runningTime));
//    }
  }


//  @VisibleForTesting
//  Pair getNameNodePathsToMove(String path) throws Exception {
//    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
//    final URI singleNs = namenodes.size() == 1 ? namenodes.iterator().next() : null;
//
//    Path target = new Path(path);
//    if (!target.isUriPathAbsolute()) {
//      throw new IllegalArgumentException("The path " + target + " is not absolute");
//    }
//    URI targetUri = target.toUri();
//    if ((targetUri.getAuthority() == null || targetUri.getScheme() == null) && singleNs == null) {
//      // each path must contains both scheme and authority information
//      // unless there is only one name service specified in the
//      // configuration
//      throw new IllegalArgumentException("The path " + target
//          + " does not contain scheme and authority thus cannot identify"
//          + " its name service");
//    }
//
//    URI key = singleNs;
//    if (singleNs == null) {
//      key = new URI(targetUri.getScheme(), targetUri.getAuthority(), null, null, null);
//      if (!namenodes.contains(key)) {
//        throw new IllegalArgumentException(
//            "Cannot resolve the path " + target
//                + ". The namenode services specified in the "
//                + "configuration: " + namenodes);
//      }
//    }
//
//    return new Pair(key, Path.getPathWithoutSchemeAndAuthority(target));
//  }

}
