package org.apache.hadoop.ssm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by root on 2/10/17.
 */
public class CommandPool {
  class Command {
    private String command;
    private LinkedBlockingQueue<String> stdOutputs;
    private Boolean isFinished;
    private Integer exitCode;

    public Command(String command) {
      this.command = command;
      stdOutputs = new LinkedBlockingQueue<String>();
      isFinished = false;
      exitCode = 0;
    }

    public String[] getOutputs() {
      String[] stdOutputsArray = stdOutputs.toArray(new String[0]);

      return stdOutputsArray;
    }

    public String getCommand() {
      return command;
    }

    public Boolean isFinished() {
      return isFinished;
    }

    public Integer getExitCode() {
      return exitCode;
    }


  }


}
