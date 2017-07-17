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
package org.smartdata.admin.tools;

import org.apache.commons.lang.WordUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.tools.TableListing;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.smartdata.admin.SmartAdmin;
import org.smartdata.conf.SmartConf;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/** Provide cmdlet line access to a FileSystem. */
@InterfaceAudience.Private
public class SmartShell extends Configured implements Tool {

  static final Log LOG = LogFactory.getLog(SmartShell.class);

  private static final int MAX_LINE_WIDTH = 80;

  private SmartAdmin client;
  private Help help;
  protected CommandFactory cmdletFactory;

  private final String usagePrefix = "Usage: ssm [generic options]";

  static final String SHELL_HTRACE_PREFIX = "ssm.shell.htrace.";

  /**
   * Default ctor with no configuration.  Be sure to invoke
   * {@link #setConf(Configuration)} with a valid configuration prior
   * to running cmdlets.
   */
  public SmartShell() {
    this(null);
  }

  /**
   * Construct a FsShell with the given configuration.  Cmdlets can be
   * executed via {@link #run(String[])}
   * @param conf the hadoop configuration
   */
  public SmartShell(Configuration conf) {
    super(conf);
  }

  protected Help getHelp() throws IOException {
    if (this.help == null){
      this.help = new Help();
    }
    return this.help;
  }

  protected void init() throws IOException {
    getConf().setQuietMode(true);
    if (cmdletFactory == null) {
      cmdletFactory = new CommandFactory(getConf());
      cmdletFactory.addObject(new Help(), "-help");
      cmdletFactory.addObject(new Usage(), "-usage");
      registerCmdlets(cmdletFactory);
    }
  }

  protected void registerCmdlets(CommandFactory factory) {
    factory.registerCommands(SmartCmdlet.class);
  }

  protected String getUsagePrefix() {
    return usagePrefix;
  }

  // NOTE: Usage/Help are inner classes to allow access to outer methods
  // that access cmdletFactory

  /**
   *  Display help for cmdlets with their short usage and long description.
   */
  protected class Usage extends SmartCmdlet {
    public static final String NAME = "usage";
    public static final String USAGE = "[cmd ...]";
    public static final String DESCRIPTION =
        "Displays the usage for given cmdlet or all cmdlets if none " +
            "is specified.";

    @Override
    protected void processRawArguments(LinkedList<String> args) {
      if (args.isEmpty()) {
        printUsage(System.out);
      } else {
        for (String arg : args) printUsage(System.out, arg);
      }
    }
  }

  /**
   * Displays short usage of cmdlets sans the long description
   */
  protected class Help extends SmartCmdlet {
    public static final String NAME = "help";
    public static final String USAGE = "[cmd ...]";
    public static final String DESCRIPTION =
        "Displays help for given cmdlet or all cmdlets if none " +
            "is specified.";

    @Override
    protected void processRawArguments(LinkedList<String> args) {
      if (args.isEmpty()) {
        printHelp(System.out);
      } else {
        for (String arg : args) printHelp(System.out, arg);
      }
    }
  }

  /*
   * The following are helper methods for getInfo().  They are defined
   * outside of the scope of the Help/Usage class because the run() method
   * needs to invoke them too.
   */

  // print all usages
  private void printUsage(PrintStream out) {
    printInfo(out, null, false);
  }

  // print one usage
  private void printUsage(PrintStream out, String cmd) {
    printInfo(out, cmd, false);
  }

  // print all helps
  private void printHelp(PrintStream out) {
    printInfo(out, null, true);
  }

  // print one help
  private void printHelp(PrintStream out, String cmd) {
    printInfo(out, cmd, true);
  }

  private void printInfo(PrintStream out, String cmd, boolean showHelp) {
    if (cmd != null) {
      // display help or usage for one cmdlet
      Command instance = cmdletFactory.getInstance("-" + cmd);
      if (instance == null) {
        throw new UnknownCmdletException(cmd);
      }
      if (showHelp) {
        printInstanceHelp(out, instance);
      } else {
        printInstanceUsage(out, instance);
      }
    } else {
      // display help or usage for all cmdlets
      out.println(getUsagePrefix());

      // display list of short usages
      List<Command> instances = new ArrayList<>();
      for (String name : cmdletFactory.getNames()) {
        Command instance = cmdletFactory.getInstance(name);
        if (!instance.isDeprecated()) {
          out.println("\t[" + instance.getUsage() + "]");
          instances.add(instance);
        }
      }
      // display long descriptions for each cmdlet
      if (showHelp) {
        for (Command instance : instances) {
          out.println();
          printInstanceHelp(out, instance);
        }
      }
      out.println();
      ToolRunner.printGenericCommandUsage(out);
    }
  }

  private void printInstanceUsage(PrintStream out, Command instance) {
    out.println(getUsagePrefix() + " " + instance.getUsage());
  }

  private void printInstanceHelp(PrintStream out, Command instance) {
    out.println(instance.getUsage() + " :");
    TableListing listing = null;
    final String prefix = "  ";
    for (String line : instance.getDescription().split("\n")) {
      if (line.matches("^[ \t]*[-<].*$")) {
        String[] segments = line.split(":");
        if (segments.length == 2) {
          if (listing == null) {
            listing = createOptionTableListing();
          }
          listing.addRow(segments[0].trim(), segments[1].trim());
          continue;
        }
      }

      // Normal literal description.
      if (listing != null) {
        for (String listingLine : listing.toString().split("\n")) {
          out.println(prefix + listingLine);
        }
        listing = null;
      }

      for (String descLine : WordUtils.wrap(
          line, MAX_LINE_WIDTH, "\n", true).split("\n")) {
        out.println(prefix + descLine);
      }
    }

    if (listing != null) {
      for (String listingLine : listing.toString().split("\n")) {
        out.println(prefix + listingLine);
      }
    }
  }

  // Creates a two-row table, the first row is for the cmdlet line option,
  // the second row is for the option description.
  private TableListing createOptionTableListing() {
    return new TableListing.Builder().addField("").addField("", true)
        .wrapWidth(MAX_LINE_WIDTH).build();
  }

  /**
   * run
   */
  @Override
  public int run(String argv[]) throws Exception {
    init();
    int exitCode = -1;
    if (argv.length < 1) {
      printUsage(System.err);
    } else {
      String cmd = argv[0];
      Command instance = null;
      try {
        instance = cmdletFactory.getInstance(cmd);
        if (instance == null) {
          throw new UnknownCmdletException();
        }

        exitCode = instance.run(Arrays.copyOfRange(argv, 1, argv.length));
      } catch (IllegalArgumentException e) {
        if (e.getMessage() == null) {
          displayError(cmd, "Null exception message");
          e.printStackTrace(System.err);
        } else {
          displayError(cmd, e.getLocalizedMessage());
        }
        printUsage(System.err);
        if (instance != null) {
          printInstanceUsage(System.err, instance);
        }
      } catch (Exception e) {
        // instance.run catches IOE, so something is REALLY wrong if here
        LOG.debug("Error", e);
        displayError(cmd, "Fatal internal error");
        e.printStackTrace(System.err);
      }
    }
    return exitCode;
  }

  private void displayError(String cmd, String message) {
    for (String line : message.split("\n")) {
      System.err.println(cmd + ": " + line);
      if (cmd.charAt(0) != '-') {
        Command instance = null;
        instance = cmdletFactory.getInstance("-" + cmd);
        if (instance != null) {
          System.err.println("Did you mean -" + cmd + "?  This cmdlet " +
              "begins with a dash.");
        }
      }
    }
  }

  public void close() throws IOException {
    if (client != null) {
      client = null;
    }
  }

  /**
   * main() has some simple utility methods
   * @param argv the cmdlet and its arguments
   * @throws Exception upon error
   */
  public static void main(String argv[]) throws Exception {
    SmartShell shell = newShellInstance();
    Configuration conf = new SmartConf();
    conf.setQuietMode(false);
    shell.setConf(conf);
    int res;
    try {
      res = ToolRunner.run(shell, argv);
    } finally {
      shell.close();
    }
    //System.exit(res);
  }

  // TODO: this should be abstract in a base class
  protected static SmartShell newShellInstance() {
    return new SmartShell();
  }

  /**
   * The default ctor signals that the cmdlet being executed does not exist,
   * while other ctor signals that a specific cmdlet does not exist.  The
   * latter is used by cmdlets that process other cmdlets, ex. -usage/-help
   */
  @SuppressWarnings("serial")
  static class UnknownCmdletException extends IllegalArgumentException {
    private final String cmd;
    UnknownCmdletException() { this(null); }
    UnknownCmdletException(String cmd) { this.cmd = cmd; }

    @Override
    public String getMessage() {
      return ((cmd != null) ? "'" + cmd + "': " : "") + "Unknown cmdlet";
    }
  }
}
