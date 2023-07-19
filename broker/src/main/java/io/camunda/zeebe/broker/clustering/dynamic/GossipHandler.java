/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic;

import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GossipHandler {
  private static final Logger LOG = LoggerFactory.getLogger(GossipHandler.class);
  private final LocalPersistedClusterState currentClusterState;

  private final Consumer<Cluster> configGossiper;

  private final ConfigChangeApplier configChangeApplier;

  public GossipHandler(
      final LocalPersistedClusterState currentClusterState,
      final Consumer<Cluster> configGossiper,
      final ConfigChangeApplier configChangeApplier) {
    this.currentClusterState = currentClusterState;
    this.configGossiper = configGossiper;
    this.configChangeApplier = configChangeApplier;
  }

  public void onRingChanged(final Cluster newCluster) {
    if (isSameAsExistingCluster(newCluster)) {
      return;
    }

    LOG.info("Current config {}", currentClusterState.getClusterState());
    final var nextConfig = merge(currentClusterState.getClusterState(), newCluster);
    currentClusterState.setClusterState(nextConfig);
    LOG.info("Update to new config {}", nextConfig);

    if (nextConfig.changes().hasPending()) {
      configChangeApplier.apply(nextConfig.changes(), this::update);
    }
  }

  public void update(final UnaryOperator<Cluster> ringTransformer) {
    final var nextRing = ringTransformer.apply(currentClusterState.getClusterState());
    if (!isSameAsExistingCluster(nextRing)) {
      // Gossip new ring
      onRingChanged(nextRing);
      configGossiper.accept(currentClusterState.getClusterState());
    }
  }

  private Cluster merge(final Cluster currentCluster, final Cluster newCluster) {
    if (currentCluster != null) {
      return currentCluster.merge(newCluster);
    } else {
      return newCluster;
    }
  }

  private boolean isSameAsExistingCluster(final Cluster newCluster) {
    // TODO
    if (currentClusterState.getClusterState() != null) {
      return currentClusterState.getClusterState().equals(newCluster);
    } else {
      return newCluster == null;
    }
  }
}
