/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic;

import io.atomix.cluster.MemberId;
import io.camunda.zeebe.broker.system.configuration.ClusterCfg;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public class GossipBasedCoordinator implements ConfigCoordinator {

  private final ScheduledExecutorService executorService;
  private final LocalPersistedClusterState persistedClusterState;
  private final ClusterCfg clusterCfg;

  private final GossipHandler gossipHandler;

  public GossipBasedCoordinator(
      final ScheduledExecutorService executorService,
      final LocalPersistedClusterState persistedClusterState,
      final ClusterCfg clusterCfg,
      final GossipHandler gossipHandler) {
    this.executorService = executorService;
    this.persistedClusterState = persistedClusterState;
    this.clusterCfg = clusterCfg;
    this.gossipHandler = gossipHandler;
  }

  @Override
  public CompletableFuture<Void> start() {
    final CompletableFuture<Void> started = new CompletableFuture<>();
    executorService.execute(
        () -> {
          if (persistedClusterState.getClusterState() == null) {
            generateNewConfiguration(clusterCfg);
            // TODO error handling
            started.complete(null);
          }
        });

    return started;
  }

  @Override
  public CompletableFuture<Void> addMember(final MemberId memberId) {
    final CompletableFuture<Void> started = new CompletableFuture<>();
    executorService.execute(
        () -> {
          tryAddMember(memberId);
          started.complete(null);
        });
    return started;
  }

  @Override
  public CompletableFuture<Void> leaveMember() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<Cluster> getCluster() {
    final CompletableFuture<Cluster> result = new CompletableFuture<>();
    executorService.execute(() -> result.complete(persistedClusterState.getClusterState()));
    return result;
  }

  private void generateNewConfiguration(final ClusterCfg clusterCfg) {
    // generate config from clusterCfg
    // persistedClusterState.setClusterState(clusterV0);
  }

  private boolean tryAddMember(final MemberId memberId) {
    // TODO: pre-check, member already does not exist, no config change plan in
    // progress etc.

    gossipHandler.update(cluster -> addMemberToCluster(cluster, memberId));
    return true;
  }

  private Cluster addMemberToCluster(final Cluster cluster, final MemberId memberId) {
    // TODO
    return null;
  }
}
