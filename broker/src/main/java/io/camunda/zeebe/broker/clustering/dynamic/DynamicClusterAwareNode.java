/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.atomix.cluster.AtomixCluster;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.utils.concurrent.AtomixThreadFactory;
import io.camunda.zeebe.broker.clustering.dynamic.claimant.GossipBasedCoordinator;
import io.camunda.zeebe.broker.clustering.dynamic.claimant.GossipBasedSSOTClusterState;
import io.camunda.zeebe.broker.system.configuration.ClusterCfg;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class DynamicClusterAwareNode {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicClusterAwareNode.class);
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
    final boolean isCoordinator = memberId.id().equals("0");

    final var threadFactory =
        new ThreadFactoryBuilder()
            .setNameFormat("node-%d")
            .setThreadFactory(new AtomixThreadFactory())
            .setUncaughtExceptionHandler(
                (t, e) -> LOG.error("Uncaught exception on " + t.getName(), e))
            .build();

    executorService = new ScheduledThreadPoolExecutor(1, threadFactory);
    executorService.execute(() -> MDC.put("actor-name", "member-" + memberId.id()));

    final LocalPersistedClusterState localPersistedClusterState =
        new FileBasedPersistedClusterState(configFile);
    final SSOTClusterState gossipSSOTClusterState =
        new GossipBasedSSOTClusterState(
            localPersistedClusterState, isCoordinator, atomixCluster.getCommunicationService());

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
            gossipHandler,
            isCoordinator);

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

  public ClusterConfigManager getConfigManager() {
    return clusterConfigManager;
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
        .setProperty("config", cluster.encode());
  }
}
