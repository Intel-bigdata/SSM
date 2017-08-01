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
package org.smartdata.actions.hdfs.move;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class OldMoverCli extends Configured implements Tool {
    static final Logger LOG = LoggerFactory.getLogger(OldMoverCli.class);

    private MoverStatus status;

    public OldMoverCli() {
        this(new MoverStatus());
    }

    public OldMoverCli(MoverStatus status) {
        this.status = status;
    }

    @VisibleForTesting
    public static Map<URI, List<Path>> getNameNodePathsToMove(Configuration conf, String... paths)
            throws Exception {
        Map<URI, List<Path>> map = Maps.newHashMap();
        Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
        if (paths == null || paths.length == 0) {
            for (URI namenode : namenodes) {
                map.put(namenode, null);
            }
            return map;
        }
        final URI singleNs = namenodes.size() == 1 ? namenodes.iterator().next() : null;
        for (String path : paths) {
            Path target = new Path(path);
            if (!target.isUriPathAbsolute()) {
                throw new IllegalArgumentException("The path " + target + " is not absolute");
            }
            URI targetUri = target.toUri();
            if ((targetUri.getAuthority() == null || targetUri.getScheme() == null) && singleNs == null) {
                // each path must contains both scheme and authority information
                // unless there is only one name service specified in the
                // configuration
                throw new IllegalArgumentException(
                        "The path "
                                + target
                                + " does not contain scheme and authority thus cannot identify"
                                + " its name service");
            }
            URI key = singleNs;
            if (singleNs == null) {
                key = new URI(targetUri.getScheme(), targetUri.getAuthority(), null, null, null);
                if (!namenodes.contains(key)) {
                    throw new IllegalArgumentException(
                            "Cannot resolve the path "
                                    + target
                                    + ". The namenode services specified in the "
                                    + "configuration: "
                                    + namenodes);
                }
            }
            List<Path> targets = map.get(key);
            if (targets == null) {
                targets = Lists.newArrayList();
                map.put(key, targets);
            }
            targets.add(Path.getPathWithoutSchemeAndAuthority(target));
        }
        return map;
    }

    @Override
    public int run(String[] args) throws Exception {
        final long startTime = Time.now();
        final Configuration conf = getConf();

        try {
            final Map<URI, List<Path>> map = getNameNodePathsToMove(conf, args);
            return OldMover.run(map, conf, status);
        } catch (IOException e) {
            LOG.info(e + ".  Exiting ...");
            return ExitStatus.IO_EXCEPTION.getExitCode();
        } catch (InterruptedException e) {
            LOG.info(e + ".  Exiting ...");
            return ExitStatus.INTERRUPTED.getExitCode();
        } catch (ParseException e) {
            LOG.info(e + ".  Exiting ...");
            return ExitStatus.ILLEGAL_ARGUMENTS.getExitCode();
        } catch (IllegalArgumentException e) {
            LOG.info(e + ".  Exiting ...");
            return ExitStatus.ILLEGAL_ARGUMENTS.getExitCode();
        } finally {
            long runningTime = Time.now() - startTime;
            LOG.info("Mover took " + StringUtils.formatTime(runningTime));
        }
    }
}

