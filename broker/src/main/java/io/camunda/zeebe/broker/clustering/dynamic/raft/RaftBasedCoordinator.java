/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic.raft;

import static io.camunda.zeebe.broker.clustering.dynamic.claimant.GossipBasedCoordinator.CONFIG_QUERY;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.raft.RaftRoleChangeListener;
import io.atomix.raft.RaftServer.Role;
import io.atomix.raft.partition.RaftPartition;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster.ClusterChangePlan;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster.ClusterState;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster.MemberState;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster.PartitionState;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster.State;
import io.camunda.zeebe.broker.clustering.dynamic.ConfigCoordinator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RaftBasedCoordinator implements ConfigCoordinator, RaftRoleChangeListener {

  public static final String SYSTEM_PARTITION_LEADER_PROPERTY_NAME = "systemPartitionLeader";
  private final ScheduledExecutorService executorService;
  private final RaftPartition raftPartition;
  private final ClusterConfigStateMachine clusterConfigStateMachine;
  private final ClusterMembershipService membershipService;
  private Role currentRole;
  private final ClusterCommunicationService clusterCommunicationService;

  public RaftBasedCoordinator(
      final ScheduledExecutorService executorService,
      final RaftPartition raftPartition,
      final ClusterConfigStateMachine clusterConfigStateMachine,
      final ClusterMembershipService membershipService,
      final ClusterCommunicationService clusterCommunicationService) {
    this.executorService = executorService;
    this.raftPartition = raftPartition;
    this.clusterConfigStateMachine = clusterConfigStateMachine;
    this.membershipService = membershipService;
    this.clusterCommunicationService = clusterCommunicationService;
  }

  @Override
  public CompletableFuture<Void> start() {
    raftPartition.getServer().addCommitListener(clusterConfigStateMachine);
    clusterConfigStateMachine.start();
    raftPartition.addRoleChangeListener(this);

    clusterCommunicationService.replyTo(
        CONFIG_QUERY, this::decodeQuery, ignore -> getCluster(), this::encodeQueryResponse);

    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> addMember(final MemberId memberId) {
    final CompletableFuture<Void> added = new CompletableFuture<>();
    executorService.execute(
        () -> {
          if (currentRole != Role.LEADER) {
            added.completeExceptionally(new IllegalStateException("Not leader"));
          } else {
            // TODO
          }
        });
    return added;
  }

  @Override
  public CompletableFuture<Void> leaveMember() {
    final CompletableFuture<Void> added = new CompletableFuture<>();
    executorService.execute(
        () -> {
          if (currentRole != Role.LEADER) {
            added.completeExceptionally(new IllegalStateException("Not leader"));
          } else {
            // TODO
          }
        });
    return added;
  }

  @Override
  public CompletableFuture<Cluster> getCluster() {
    final CompletableFuture<Cluster> result = new CompletableFuture<>();
    executorService.execute(
        () -> {
          if (currentRole != Role.LEADER) {
            result.completeExceptionally(new IllegalStateException("not leader"));
          } else {
            clusterConfigStateMachine
                .getCluster()
                .whenComplete(
                    (cluster, error) -> {
                      if (error == null) {
                        if (cluster == null) {
                          result.completeExceptionally(new RuntimeException("uninitialized"));
                        } else {
                          result.complete(cluster);
                        }
                      } else {
                        result.completeExceptionally(error);
                      }
                    });
          }
        });
    return result;
  }

  private byte[] encodeQueryResponse(final Cluster response) {
    return response.encodeAsBytes();
  }

  private byte[] decodeQuery(final byte[] bytes) {
    return bytes;
  }

  @Override
  public void onNewRole(final Role newRole, final long newTerm) {
    executorService.execute(
        () -> {
          switch (newRole) {
            case LEADER -> transitionToLeader();
            case FOLLOWER -> transitionToFollower();
            default -> {}
          }
        });
  }

  private void transitionToFollower() {
    currentRole = Role.FOLLOWER;
    membershipService.getLocalMember().properties().remove(SYSTEM_PARTITION_LEADER_PROPERTY_NAME);
  }

  private void transitionToLeader() {
    clusterConfigStateMachine.onCommit(
        0); // should get the latest state before transitioning to leader, to ensure that queries
    // returns the latest
    currentRole = Role.LEADER;
    membershipService
        .getLocalMember()
        .properties()
        .setProperty(
            SYSTEM_PARTITION_LEADER_PROPERTY_NAME, membershipService.getLocalMember().id().id());

    initialize();
  }

  private void initialize() {
    clusterConfigStateMachine
        .getCluster()
        .whenComplete(
            (cluster, error) -> {
              if (error == null && cluster == null) {
                generateInitialConfig();
              } else if (error != null) {
                executorService.schedule(this::initialize, 1, TimeUnit.SECONDS);
              }
            });
  }

  private void generateInitialConfig() {
    // TODO: generate config from clusterCfg.

    // For now use hard-coded config
    final var members =
        IntStream.of(0, 1, 2)
            .mapToObj(i -> MemberId.from(String.valueOf(i)))
            .collect(Collectors.toMap(Function.identity(), this::getConfigOfMember));
    final var clusterState = new ClusterState(members);
    final var initialConfig = new Cluster(0, clusterState, new ClusterChangePlan(0, List.of()));
    clusterConfigStateMachine.setCluster(initialConfig);
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

  public boolean isLeader() {
    return raftPartition.getServer().getAppender().isPresent();
  }

  public void updateCluster(final Cluster cluster) {
    executorService.execute(
        () -> {
          if (currentRole == Role.LEADER) {
            clusterConfigStateMachine.setCluster(cluster);
          }
        });
  }
}
