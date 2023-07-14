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

public class GossipBasedCoordinator implements ConfigCoordinator {

  private final PersistedClusterState persistedClusterState;
  private final ClusterCfg clusterCfg;

  private final GossipHandler gossipHandler;

  public GossipBasedCoordinator(
      final PersistedClusterState persistedClusterState,
      final ClusterCfg clusterCfg,
      final GossipHandler gossipHandler) {
    this.persistedClusterState = persistedClusterState;
    this.clusterCfg = clusterCfg;
    this.gossipHandler = gossipHandler;
  }

  @Override
  public CompletableFuture<Void> start() {
    if (persistedClusterState.getClusterState() == null) {
      generateNewConfiguration(clusterCfg);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Boolean> addMember() {
    return null;
  }

  private void generateNewConfiguration(final ClusterCfg clusterCfg) {
    // TODO register listener outside
    // generate config from clusterCfg
    // persistedClusterState.setClusterState(clusterV0);
  }

  private boolean addMember(final MemberId memberId) {
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
