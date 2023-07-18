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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterConfigManager implements ClusterMembershipEventListener {
  private static final long RETRY_DELAY = 5_000; // 5 seconds

  private static final Logger LOG = LoggerFactory.getLogger(ClusterConfigManager.class);
  private final ScheduledExecutorService executorService;
  private final LocalPersistedClusterState persistedClusterState;
  private final SSOTClusterState ssotClusterState;
  private final GossipHandler gossipHandler;

  private final CompletableFuture<Boolean> started = new CompletableFuture<>();

  public ClusterConfigManager(
      final ScheduledExecutorService executorService,
      final ClusterCfg clusterCfg,
      final LocalPersistedClusterState persistedClusterState,
      final SSOTClusterState ssotClusterState,
      final GossipHandler gossipHandler) {
    this.executorService = executorService;
    this.persistedClusterState = persistedClusterState;
    this.ssotClusterState = ssotClusterState;
    this.gossipHandler = gossipHandler;
    executorService.execute(() -> initialize(clusterCfg));
  }

  private void initialize(final ClusterCfg clusterCfg) {
    if (persistedClusterState.getClusterState() == null) {
      LOG.info("No local cluster stat found, asking the coordinator");
      ssotClusterState
          .getClusterState()
          .thenAccept(
              newCluster -> {
                gossipHandler.onRingChanged(newCluster);
                started.complete(true);
              })
          .exceptionally(
              error -> {
                LOG.info("Failed to get cluster config from coordinator, retrying", error);
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
    executorService.execute(
        () -> {
          final var encodedConfig = event.subject().properties().get("config");
          if (encodedConfig == null) {
            return;
          }

          final Cluster newCluster = Cluster.decode((byte[]) encodedConfig);
          if (!persistedClusterState.getClusterState().equals(newCluster)) {
            LOG.info(
                "Received different cluster config via gossip from member {}. Updating.",
                event.subject().id().id());
            gossipHandler.onRingChanged(newCluster);
          }
        });
  }
}
