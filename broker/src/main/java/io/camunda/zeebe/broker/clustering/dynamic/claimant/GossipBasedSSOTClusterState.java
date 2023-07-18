/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic.claimant;

import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster;
import io.camunda.zeebe.broker.clustering.dynamic.LocalPersistedClusterState;
import io.camunda.zeebe.broker.clustering.dynamic.SSOTClusterState;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GossipBasedSSOTClusterState implements SSOTClusterState {
  private static final Logger LOG = LoggerFactory.getLogger(GossipBasedSSOTClusterState.class);
  private final LocalPersistedClusterState clusterState;
  private final boolean isCoordinator;

  private final ClusterCommunicationService communicationService;
  private final MemberId coordinatorId =
      MemberId.from("0"); // TODO: make it configurable or dynamic

  public GossipBasedSSOTClusterState(
      final LocalPersistedClusterState persistedClusterState,
      final boolean isCoordinator,
      final ClusterCommunicationService communicationService) {
    clusterState = persistedClusterState;
    this.isCoordinator = isCoordinator;
    this.communicationService = communicationService;
  }

  @Override
  public CompletableFuture<Cluster> getClusterState() {
    if (isCoordinator()) {
      return CompletableFuture.completedFuture(clusterState.getClusterState());
    } else {
      // Find the coordinator, and send a remote request
      return communicationService.send(
          GossipBasedCoordinator.CONFIG_QUERY,
          new byte[0],
          Function.identity(),
          Cluster::decode,
          coordinatorId,
          Duration.ofSeconds(5));
    }
  }

  private boolean isCoordinator() {
    // TODO
    return isCoordinator;
  }
}
