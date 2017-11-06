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
package org.smartdata.server.utils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GenericOptionsParser {
  private Configuration conf;
  private CommandLine cmdLine;

  public GenericOptionsParser(Configuration conf, String[] args) throws IOException {
    this(conf, new Options(), args);
  }

  public GenericOptionsParser(Configuration conf, Options options, String[] args)
      throws IOException {
    this.conf = conf;
    parseGeneralOptions(options, args);
  }

  public String[] getRemainingArgs() {
    return (cmdLine == null) ? new String[] {} : cmdLine.getArgs();
  }

  public Configuration getConfiguration() {
    return conf;
  }

  public CommandLine getCommandLine() {
    return cmdLine;
  }

  private Options buildGeneralOptions(Options opts) {
    Option property =
        OptionBuilder.withArgName("property=value")
            .hasArg()
            .withDescription("use value for given property")
            .create('D');
    opts.addOption(property);

    return opts;
  }

  private void processGeneralOptions(CommandLine line) throws IOException {
    if (line.hasOption('D')) {
      String[] property = line.getOptionValues('D');
      for (String prop : property) {
        String[] keyval = prop.split("=", 2);
        if (keyval.length == 2) {
          conf.set(keyval[0], keyval[1], "from command line");
        }
      }
    }
  }

  private String[] preProcessForWindows(String[] args) {
    boolean isWindows = false;
    String osName = System.getProperty("os.name");
    if (osName.startsWith("Windows")) {
      isWindows = true;
    }
    if (!isWindows) {
      return args;
    }
    if (args == null) {
      return null;
    }
    List<String> newArgs = new ArrayList<String>(args.length);
    for (int i = 0; i < args.length; i++) {
      String prop = null;
      if (args[i].equals("-D")) {
        newArgs.add(args[i]);
        if (i < args.length - 1) {
          prop = args[++i];
        }
      } else if (args[i].startsWith("-D")) {
        prop = args[i];
      } else {
        newArgs.add(args[i]);
      }
      if (prop != null) {
        if (prop.contains("=")) {
          // everything good
        } else {
          if (i < args.length - 1) {
            prop += "=" + args[++i];
          }
        }
        newArgs.add(prop);
      }
    }

    return newArgs.toArray(new String[newArgs.size()]);
  }

  private void parseGeneralOptions(Options opts, String[] args) throws IOException {
    opts = buildGeneralOptions(opts);
    CommandLineParser parser = new GnuParser();
    try {
      cmdLine = parser.parse(opts, preProcessForWindows(args), true);
      processGeneralOptions(cmdLine);
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("general options are: ", opts);
    }
  }
}
