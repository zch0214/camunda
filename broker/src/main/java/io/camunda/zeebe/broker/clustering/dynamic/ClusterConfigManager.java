/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic;

import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.camunda.zeebe.broker.system.configuration.ClusterCfg;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ClusterConfigManager implements ClusterMembershipEventListener {
  private static final long RETRY_DELAY = 5_000; // 5 seconds
  private final ScheduledExecutorService executorService;
  private final LocalPersistedClusterState persistedClusterState;
  private final SSOTClusterState ssotClusterState;
  private final GossipHandler gossipHandler;

  private final CompletableFuture<Boolean> started = new CompletableFuture<>();

  public ClusterConfigManager(
      final ScheduledExecutorService executorService,
      final ClusterCfg clusterCfg,
      final SSOTClusterState ssotClusterState) {
    this.executorService = executorService;
    this.ssotClusterState = ssotClusterState;
    persistedClusterState = new FileBasedPersistedClusterState();
    executorService.execute(() -> initialize(clusterCfg));
    gossipHandler = null;
  }

  private void initialize(final ClusterCfg clusterCfg) {
    if (persistedClusterState.getClusterState() == null) {
      ssotClusterState
          .getClusterState()
          .thenAccept(
              newCluster -> {
                gossipHandler.onRingChanged(newCluster);
                started.complete(true);
              })
          .exceptionally(
              error -> {
                executorService.schedule(
                    () -> initialize(clusterCfg), RETRY_DELAY, TimeUnit.MILLISECONDS);
                return null;
              });
    } else {
      // local config available use that
      started.complete(true);
    }
  }

  @Override
  public void event(final ClusterMembershipEvent event) {
    // TODO, on metadata changed, read cluster config from the event, and update local if necessary
    executorService.execute(
        () -> {
          final Cluster newCluster = null; // TODO: readFromEvent
          if (!persistedClusterState.getClusterState().equals(newCluster)) {
            gossipHandler.onRingChanged(newCluster);
          }
        });
  }
}
