/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic.raft;

import static io.camunda.zeebe.broker.clustering.dynamic.raft.RaftBasedCoordinator.SYSTEM_PARTITION_LEADER_PROPERTY_NAME;

import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster;
import io.camunda.zeebe.broker.clustering.dynamic.SSOTClusterState;
import io.camunda.zeebe.broker.clustering.dynamic.claimant.GossipBasedCoordinator;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class RaftBasedSSOTClusterState implements SSOTClusterState, ClusterMembershipEventListener {

  private final Optional<RaftBasedCoordinator> raftBasedCoordinator;
  private final ClusterCommunicationService communicationService;
  private MemberId currentLeader;

  public RaftBasedSSOTClusterState(
      final Optional<RaftBasedCoordinator> raftBasedCoordinator,
      final ClusterCommunicationService communicationService) {
    this.raftBasedCoordinator = raftBasedCoordinator;
    this.communicationService = communicationService;
  }

  @Override
  public CompletableFuture<Cluster> getClusterState() {
    if (isCoordinator()) {
      return raftBasedCoordinator.get().getCluster();
    } else {
      final MemberId coordinatorId = getLeaderOfSystemPartition();
      if (coordinatorId == null) {
        return CompletableFuture.failedFuture(new RuntimeException("Leader not known"));
      }
      return communicationService.send(
          GossipBasedCoordinator.CONFIG_QUERY,
          new byte[0],
          Function.identity(),
          Cluster::decode,
          coordinatorId,
          Duration.ofSeconds(5));
    }
  }

  private MemberId getLeaderOfSystemPartition() {
    return currentLeader;
  }

  private boolean isCoordinator() {
    return raftBasedCoordinator.isPresent() && raftBasedCoordinator.get().isLeader();
  }

  @Override
  public void event(final ClusterMembershipEvent event) {
    final var systemPartitionLeader =
        event.subject().properties().getProperty(SYSTEM_PARTITION_LEADER_PROPERTY_NAME);
    if (systemPartitionLeader != null) {
      // TODO: handle out of order updates by checking leader terms etc.
      currentLeader = MemberId.from(systemPartitionLeader);
    }
  }
}
