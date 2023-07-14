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

public class GossipHandler {
  private LocalPersistedClusterState currentClusterState;

  private Consumer<Cluster> ringGossiper;

  public void onRingChanged(final Cluster newCluster) {
    if (isSameAsExistingCluster(newCluster)) {
      return;
    }

    final var nextRing = merge(currentClusterState.getClusterState(), newCluster);
    currentClusterState.setClusterState(nextRing);

    if (nextRing.changes().hasPending()) {
      // TODO
    }
  }

  public void update(final UnaryOperator<Cluster> ringTransformer) {
    final var nextRing = ringTransformer.apply(currentClusterState.getClusterState());
    if (!isSameAsExistingCluster(nextRing)) {
      // Gossip new ring
      onRingChanged(nextRing);
      ringGossiper.accept(currentClusterState.getClusterState());
    }
  }

  private Cluster merge(final Cluster ring, final Cluster newCluster) {
    // TODO
    return ring;
  }

  private boolean isSameAsExistingCluster(final Cluster newCluster) {
    // TODO
    return currentClusterState.getClusterState().equals(newCluster);
  }
}
