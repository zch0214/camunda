/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic.raft;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.raft.RaftRoleChangeListener;
import io.atomix.raft.RaftServer.Role;
import io.atomix.raft.partition.RaftPartition;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster;
import io.camunda.zeebe.broker.clustering.dynamic.ConfigCoordinator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public class RaftBasedCoordinator implements ConfigCoordinator, RaftRoleChangeListener {

  public static final String SYSTEM_PARTITION_LEADER_PROPERTY_NAME = "systemPartitionLeader";
  private final ScheduledExecutorService executorService;
  private final RaftPartition raftPartition;
  private final ClusterConfigStateMachine clusterConfigStateMachine;
  private final ClusterMembershipService membershipService;
  private Role currentRole;

  public RaftBasedCoordinator(
      final ScheduledExecutorService executorService,
      final RaftPartition raftPartition,
      final ClusterConfigStateMachine clusterConfigStateMachine,
      final ClusterMembershipService membershipService) {
    this.executorService = executorService;
    this.raftPartition = raftPartition;
    this.clusterConfigStateMachine = clusterConfigStateMachine;
    this.membershipService = membershipService;
  }

  @Override
  public CompletableFuture<Void> start() {
    raftPartition.getServer().addCommitListener(clusterConfigStateMachine);
    clusterConfigStateMachine.start();
    raftPartition.addRoleChangeListener(this);
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

  @Override
  public void onNewRole(final Role newRole, final long newTerm) {
    executorService.execute(
        () -> {
          switch (newRole) {
            case LEADER -> transitionToLeader();
            case FOLLOWER -> transitionToFollower();
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
  }

  public boolean isLeader() {
    return raftPartition.getServer().getAppender().isPresent();
  }
}
