package org.smartdata.server.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.AbstractService;
import org.smartdata.actions.SmartAction;
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.FileDiff;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class DisasterRecoveryManager extends AbstractService {
  static final Logger LOG = LoggerFactory.getLogger(DisasterRecoveryManager.class);

  private MetaStore metaStore;
  private Queue<FileDiff> pendingDR;
  private List<Long> runningDR;

  public DisasterRecoveryManager(ServerContext context) {
    super(context);

    this.metaStore = context.getMetaStore();
    this.runningDR = new ArrayList<>();
    this.pendingDR = new LinkedBlockingQueue<>();
  }

  @Override
  public void init() throws IOException {
  }

  @Override
  public void start() throws IOException {
  }

  @Override
  public void stop() throws IOException {
  }

}
