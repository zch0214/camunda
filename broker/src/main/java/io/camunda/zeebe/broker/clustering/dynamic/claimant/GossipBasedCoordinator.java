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
import io.camunda.zeebe.broker.clustering.dynamic.Cluster.ClusterChangeOperation;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster.ClusterChangeOperationEnum;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster.ClusterChangePlan;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster.ClusterState;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster.MemberState;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster.PartitionState;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster.State;
import io.camunda.zeebe.broker.clustering.dynamic.ConfigCoordinator;
import io.camunda.zeebe.broker.clustering.dynamic.GossipHandler;
import io.camunda.zeebe.broker.clustering.dynamic.LocalPersistedClusterState;
import io.camunda.zeebe.broker.system.configuration.ClusterCfg;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GossipBasedCoordinator implements ConfigCoordinator {

  public static final String CONFIG_QUERY = "get-latest-config";
  private static final Logger LOG = LoggerFactory.getLogger(GossipBasedCoordinator.class);
  private final ScheduledExecutorService executorService;
  private final LocalPersistedClusterState persistedClusterState;
  private final ClusterCfg clusterCfg;

  private final GossipHandler gossipHandler;

  private final ClusterCommunicationService clusterCommunicationService;

  public GossipBasedCoordinator(
      final ScheduledExecutorService executorService,
      final LocalPersistedClusterState persistedClusterState,
      final ClusterCfg clusterCfg,
      final GossipHandler gossipHandler,
      final ClusterCommunicationService clusterCommunicationService) {
    this.executorService = executorService;
    this.persistedClusterState = persistedClusterState;
    this.clusterCfg = clusterCfg;
    this.gossipHandler = gossipHandler;
    this.clusterCommunicationService = clusterCommunicationService;
  }

  @Override
  public CompletableFuture<Void> start() {
    final CompletableFuture<Void> started = new CompletableFuture<>();
    executorService.execute(
        () -> {
          if (persistedClusterState.getClusterState() == null) {
            LOG.info("No persisted configuration found. Generating new config");
            generateNewConfiguration(clusterCfg);

            clusterCommunicationService.replyTo(
                CONFIG_QUERY,
                this::decodeQuery,
                this::getConfig,
                this::encodeQueryResponse,
                executorService);
            // TODO error handling
            started.complete(null);
          }
        });
    return started;
  }

  @Override
  public CompletableFuture<Void> addMember(final MemberId memberId) {
    final CompletableFuture<Void> started = new CompletableFuture<>();
    executorService.execute(
        () -> {
          tryAddMember(memberId);
          started.complete(null);
        });
    return started;
  }

  @Override
  public CompletableFuture<Void> leaveMember() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<Cluster> getCluster() {
    final CompletableFuture<Cluster> result = new CompletableFuture<>();
    executorService.execute(() -> result.complete(persistedClusterState.getClusterState()));
    return result;
  }

  private byte[] encodeQueryResponse(final Cluster response) {

    return response.encodeAsBytes();
  }

  private Cluster getConfig(final MemberId memberId, final byte[] ignore) {
    if (persistedClusterState.getClusterState() != null) {
      return persistedClusterState.getClusterState();
    } else {
      throw new RuntimeException("Config not found");
    }
  }

  private byte[] decodeQuery(final byte[] bytes) {
    return bytes;
  }

  private void generateNewConfiguration(final ClusterCfg clusterCfg) {
    // TODO: generate config from clusterCfg.

    // For now use hard-coded config
    final var members =
        IntStream.of(0, 1, 2)
            .mapToObj(i -> MemberId.from(String.valueOf(i)))
            .collect(Collectors.toMap(Function.identity(), this::getConfigOfMember));
    final var clusterState = new ClusterState(members);
    final var initialConfig = new Cluster(0, clusterState, new ClusterChangePlan(0, List.of()));
    persistedClusterState.setClusterState(initialConfig);
  }

  private MemberState getConfigOfMember(final MemberId memberId) {
    if (memberId.id().equals("0")) {
      return new MemberState(0, State.ACTIVE, Map.of(1, new PartitionState(State.ACTIVE, 1)));
    } else if (memberId.id().equals("1")) {
      return new MemberState(0, State.ACTIVE, Map.of(2, new PartitionState(State.ACTIVE, 1)));
    } else if (memberId.id().equals("2")) {
      return new MemberState(0, State.ACTIVE, Map.of(3, new PartitionState(State.ACTIVE, 1)));
    }
    throw new IllegalStateException();
  }

  private boolean tryAddMember(final MemberId memberId) {
    // TODO: pre-check, member already does not exist, no config change plan in
    // progress etc.

    gossipHandler.update(cluster -> addMemberToCluster(cluster, memberId));
    return true;
  }

  private Cluster addMemberToCluster(final Cluster cluster, final MemberId memberId) {
    final ClusterChangeOperation operation =
        new ClusterChangeOperation(memberId.id(), ClusterChangeOperationEnum.JOIN);
    final long newVersion = cluster.version() + 1;
    return new Cluster(
        newVersion, // increment version
        cluster.clusterState(), // no change to cluster config yet
        new Cluster.ClusterChangePlan(
            newVersion, List.of(operation))); // Change plan consists of just one change
  }
}
