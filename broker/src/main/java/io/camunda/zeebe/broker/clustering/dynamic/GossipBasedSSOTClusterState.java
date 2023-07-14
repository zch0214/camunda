/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic;

import java.util.concurrent.CompletableFuture;

public class GossipBasedSSOTClusterState implements SSOTClusterState {
  private final LocalPersistedClusterState clusterState;

  public GossipBasedSSOTClusterState(final LocalPersistedClusterState persistedClusterState) {
    clusterState = persistedClusterState;
  }

  @Override
  public CompletableFuture<Cluster> getClusterState() {
    if (isCoordinator()) {
      return CompletableFuture.completedFuture(clusterState.getClusterState());
    } else {
      // Find the coordinator, and send a remote request
      throw new UnsupportedOperationException("To be implemented");
    }
  }

  private boolean isCoordinator() {
    // TODO
    return false;
  }
}
