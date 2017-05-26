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
package org.apache.hadoop.smart.command.actions.mover.defaultmover;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.tools.javac.util.Log;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.smart.command.actions.mover.Status;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * A mover client to run Mover.
 */
public class MoverCli extends Configured implements Tool {
  static final Logger LOG = LoggerFactory.getLogger(MoverCli.class);

  private static final String USAGE = "Usage: hdfs mover "
          + "[-p <files/dirs> | -f <local file>]"
          + "\n\t-p <files/dirs>\ta space separated list of HDFS files/dirs to migrate."
          + "\n\t-f <local file>\ta local file containing a list of HDFS files/dirs to migrate.";

  private MoverStatus status;

  public MoverCli() {
    status = new MoverStatus();
  }

  public MoverCli(Status status) {
    if (status instanceof MoverStatus) {
      this.status = (MoverStatus)status;
    }
    else {
      throw new IllegalArgumentException("MoverCli must use MoverStatus to"
          + "initialize");
    }
  }

  private static Options buildCliOptions() {
    Options opts = new Options();
    Option file = OptionBuilder.withArgName("pathsFile").hasArg()
            .withDescription("a local file containing files/dirs to migrate")
            .create("f");
    Option paths = OptionBuilder.withArgName("paths").hasArgs()
            .withDescription("specify space separated files/dirs to migrate")
            .create("p");
    OptionGroup group = new OptionGroup();
    group.addOption(file);
    group.addOption(paths);
    opts.addOptionGroup(group);
    return opts;
  }

  private static String[] readPathFile(String file) throws IOException {
    List<String> list = Lists.newArrayList();
    BufferedReader reader = new BufferedReader(
            new InputStreamReader(new FileInputStream(file), "UTF-8"));
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!line.trim().isEmpty()) {
          list.add(line);
        }
      }
    } finally {
      IOUtils.cleanup(null, reader);
    }
    return list.toArray(new String[list.size()]);
  }

  private static Map<URI, List<Path>> getNameNodePaths(CommandLine line,
      Configuration conf) throws Exception {
    Map<URI, List<Path>> map = Maps.newHashMap();
    String[] paths = null;
    if (line.hasOption("f")) {
      paths = readPathFile(line.getOptionValue("f"));
    } else if (line.hasOption("p")) {
      paths = line.getOptionValues("p");
    }
    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    if (paths == null || paths.length == 0) {
      for (URI namenode : namenodes) {
        map.put(namenode, null);
      }
      return map;
    }
    final URI singleNs = namenodes.size() == 1 ?
            namenodes.iterator().next() : null;
    for (String path : paths) {
      Path target = new Path(path);
      if (!target.isUriPathAbsolute()) {
        throw new IllegalArgumentException("The path " + target
                + " is not absolute");
      }
      URI targetUri = target.toUri();
      if ((targetUri.getAuthority() == null || targetUri.getScheme() ==
              null) && singleNs == null) {
        // each path must contains both scheme and authority information
        // unless there is only one name service specified in the
        // configuration
        throw new IllegalArgumentException("The path " + target
                + " does not contain scheme and authority thus cannot identify"
                + " its name service");
      }
      URI key = singleNs;
      if (singleNs == null) {
        key = new URI(targetUri.getScheme(), targetUri.getAuthority(),
                null, null, null);
        if (!namenodes.contains(key)) {
          throw new IllegalArgumentException("Cannot resolve the path " +
                  target + ". The namenode services specified in the " +
                  "configuration: " + namenodes);
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

  @VisibleForTesting
  static Map<URI, List<Path>> getNameNodePathsToMove(Configuration conf,
      String... args) throws Exception {
    final Options opts = buildCliOptions();
    CommandLineParser parser = new GnuParser();
    CommandLine commandLine = parser.parse(opts, args, true);
    return getNameNodePaths(commandLine, conf);
  }

  @Override
  public int run(String[] args) throws Exception {
    final long startTime = Time.monotonicNow();
    status.setStartTime(startTime);
    final Configuration conf = getConf();

    try {
      final Map<URI, List<Path>> map = getNameNodePathsToMove(conf, args);
      return Mover.run(map, conf, status);
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
      status.setIsFinished();
      long runningTime = Time.monotonicNow() - startTime;
      Log.format("%-24s ", DateFormat.getDateTimeInstance().format(new Date()));
      LOG.info("Mover took " + StringUtils.formatTime(runningTime));
      status.setTotalDuration(runningTime);
    }
  }
}
