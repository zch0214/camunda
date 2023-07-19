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
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.utils.concurrent.AtomixThreadFactory;
import io.camunda.zeebe.broker.clustering.dynamic.raft.ClusterConfigStateMachine;
import io.camunda.zeebe.broker.clustering.dynamic.raft.RaftBasedCoordinator;
import io.camunda.zeebe.broker.clustering.dynamic.raft.RaftBasedLocalPersistedClusterState;
import io.camunda.zeebe.broker.clustering.dynamic.raft.RaftBasedSSOTClusterState;
import io.camunda.zeebe.broker.clustering.dynamic.raft.SystemPartitionFactory;
import io.camunda.zeebe.broker.system.configuration.ClusterCfg;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class DynamicClusterAwareNodeWithRaft {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicClusterAwareNodeWithRaft.class);
  private final MemberId memberId;
  private final AtomixCluster atomixCluster;
  private final ScheduledExecutorService executorService;
  private final ClusterConfigManager clusterConfigManager;
  private RaftBasedCoordinator coordinator;
  private final RaftBasedSSOTClusterState raftBasedSSOTClusterState;

  public DynamicClusterAwareNodeWithRaft(
      final MemberId memberId,
      final ClusterCfg clusterCfg,
      final Path configFile,
      final Path raftPath,
      final AtomixCluster atomixCluster) {
    this.memberId = memberId;
    this.atomixCluster = atomixCluster;
    final boolean isCoordinator = memberId.id().equals("0");

    final var threadFactory =
        new ThreadFactoryBuilder()
            .setNameFormat("thread-%d")
            .setThreadFactory(new AtomixThreadFactory())
            .setUncaughtExceptionHandler(
                (t, e) -> LOG.error("Uncaught exception on " + t.getName(), e))
            .build();

    executorService = new ScheduledThreadPoolExecutor(1, threadFactory);
    executorService.execute(() -> MDC.put("actor-name", "member-" + memberId.id()));

    final RaftBasedLocalPersistedClusterState localPersistedClusterState =
        new RaftBasedLocalPersistedClusterState(new FileBasedPersistedClusterState(configFile));

    final ConfigChangeApplier configChangeApplier = new ConfigChangeApplier(memberId);
    final GossipHandler gossipHandler =
        new GossipHandler(
            localPersistedClusterState, this::gossipConfigUpdate, configChangeApplier);

    final var optionalRaftBasedCoordinator =
        tryCreateSystemPartition(
            raftPath,
            atomixCluster.getMembershipService(),
            atomixCluster.getCommunicationService(),
            gossipHandler);
    localPersistedClusterState.setOptionalCoordinator(optionalRaftBasedCoordinator);
    raftBasedSSOTClusterState =
        new RaftBasedSSOTClusterState(
            optionalRaftBasedCoordinator, atomixCluster.getCommunicationService());
    atomixCluster.getMembershipService().addListener(raftBasedSSOTClusterState);

    clusterConfigManager =
        new ClusterConfigManager(
            executorService,
            clusterCfg,
            localPersistedClusterState,
            raftBasedSSOTClusterState,
            gossipHandler,
            isCoordinator);

    atomixCluster.getMembershipService().addListener(clusterConfigManager);
  }

  private Optional<RaftBasedCoordinator> tryCreateSystemPartition(
      final Path path,
      final ClusterMembershipService membershipService,
      final ClusterCommunicationService communicationService,
      final GossipHandler gossipHandler) {
    final var systemPartition =
        new SystemPartitionFactory().start(path, membershipService, communicationService);
    return systemPartition.map(
        partition -> {
          coordinator =
              new RaftBasedCoordinator(
                  executorService,
                  partition,
                  new ClusterConfigStateMachine(executorService, partition),
                  membershipService,
                  communicationService,
                  gossipHandler);
          coordinator.start();
          return coordinator;
        });
  }

  public Optional<ConfigCoordinator> getCoordinator() {
    return Optional.ofNullable(coordinator);
  }

  public ClusterConfigManager getConfigManager() {
    return clusterConfigManager;
  }

  private void gossipConfigUpdate(final Cluster cluster) {
    atomixCluster
        .getMembershipService()
        .getLocalMember()
        .properties()
        .setProperty("config", cluster.encode());
  }

  public CompletableFuture<Cluster> getCluster() {
    return raftBasedSSOTClusterState.getClusterState();
  }

  public CompletableFuture<Cluster> getLocalCluster() {
    return coordinator.getCluster();
  }

  public void stop() {
    // TODO: properly close raft partition group
    executorService.shutdown();
    atomixCluster.stop().join();
  }
}
