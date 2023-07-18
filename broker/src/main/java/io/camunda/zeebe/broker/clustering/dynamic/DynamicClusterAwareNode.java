/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic;

import io.atomix.cluster.AtomixCluster;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.camunda.zeebe.broker.clustering.dynamic.claimant.GossipBasedCoordinator;
import io.camunda.zeebe.broker.clustering.dynamic.claimant.GossipBasedSSOTClusterState;
import io.camunda.zeebe.broker.system.configuration.ClusterCfg;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class DynamicClusterAwareNode {

  private final MemberId memberId;
  private final AtomixCluster atomixCluster;
  private final ScheduledExecutorService executorService;
  private final ClusterConfigManager clusterConfigManager;
  private GossipBasedCoordinator coordinator;

  public DynamicClusterAwareNode(
      final MemberId memberId,
      final ClusterCfg clusterCfg,
      final Path configFile,
      final AtomixCluster atomixCluster) {
    this.memberId = memberId;
    this.atomixCluster = atomixCluster;
    executorService = new ScheduledThreadPoolExecutor(1);
    final LocalPersistedClusterState localPersistedClusterState =
        new FileBasedPersistedClusterState(configFile);
    final SSOTClusterState gossipSSOTClusterState =
        new GossipBasedSSOTClusterState(
            localPersistedClusterState,
            memberId.id().equals("0"),
            atomixCluster.getCommunicationService());

    final ConfigChangeApplier configChangeApplier = new ConfigChangeApplier(memberId);
    final GossipHandler gossipHandler =
        new GossipHandler(
            localPersistedClusterState, this::gossipConfigUpdate, configChangeApplier);
    clusterConfigManager =
        new ClusterConfigManager(
            executorService,
            clusterCfg,
            localPersistedClusterState,
            gossipSSOTClusterState,
            gossipHandler);

    atomixCluster.getMembershipService().addListener(clusterConfigManager);

    tryStartCoordinator(
        localPersistedClusterState,
        clusterCfg,
        atomixCluster.getCommunicationService(),
        gossipHandler);
  }

  public Optional<ConfigCoordinator> getCoordinator() {
    return Optional.ofNullable(coordinator);
  }

  private void tryStartCoordinator(
      final LocalPersistedClusterState persistedClusterState,
      final ClusterCfg clusterCfg,
      final ClusterCommunicationService communicationService,
      final GossipHandler gossipHandler) {
    final boolean isCoordinator = memberId.id().equals("0");
    if (!isCoordinator) {
      return;
    }

    coordinator =
        new GossipBasedCoordinator(
            executorService,
            persistedClusterState,
            clusterCfg,
            gossipHandler,
            communicationService);
    coordinator.start();
  }

  private void gossipConfigUpdate(final Cluster cluster) {
    atomixCluster
        .getMembershipService()
        .getLocalMember()
        .properties()
        .put("config", cluster.encode());
  }
}
