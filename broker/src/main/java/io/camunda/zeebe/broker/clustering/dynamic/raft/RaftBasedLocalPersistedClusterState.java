/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic.raft;

import io.camunda.zeebe.broker.clustering.dynamic.Cluster;
import io.camunda.zeebe.broker.clustering.dynamic.FileBasedPersistedClusterState;
import io.camunda.zeebe.broker.clustering.dynamic.LocalPersistedClusterState;
import java.util.Optional;

public class RaftBasedLocalPersistedClusterState implements LocalPersistedClusterState {

  private final FileBasedPersistedClusterState localPersistedState;

  private final Optional<RaftBasedCoordinator> optionalCoordinator;

  public RaftBasedLocalPersistedClusterState(
      final FileBasedPersistedClusterState localPersistedState,
      final Optional<RaftBasedCoordinator> optionalCoordinator) {
    this.localPersistedState = localPersistedState;
    this.optionalCoordinator = optionalCoordinator;
  }

  @Override
  public Cluster getClusterState() {
    return localPersistedState.getClusterState();
  }

  @Override
  public void setClusterState(final Cluster cluster) {
    // Ensure any updates received via gossip is also updated in system partition. Only needed if we
    // use a combination of raft and gossip where raft leader initiates configuration change, but
    // use distributed coordination via gossip to apply configuration changes. Alternate is to only
    // allow raft leader to initiate and apply configuration changes.
    optionalCoordinator.ifPresent(c -> c.updateCluster(cluster));
    localPersistedState.setClusterState(cluster);
  }
}
