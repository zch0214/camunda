/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic.raft;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.partition.impl.DefaultPartitionService;
import io.atomix.raft.partition.RaftPartition;
import io.atomix.raft.partition.RaftPartitionGroup;
import io.atomix.raft.partition.RaftPartitionGroup.Builder;
import io.camunda.zeebe.broker.partitioning.PartitionManagerImpl;
import io.camunda.zeebe.scheduler.future.ActorFuture;
import io.camunda.zeebe.scheduler.future.CompletableActorFuture;
import io.camunda.zeebe.snapshots.PersistedSnapshot;
import io.camunda.zeebe.snapshots.PersistedSnapshotListener;
import io.camunda.zeebe.snapshots.ReceivableSnapshotStore;
import io.camunda.zeebe.snapshots.ReceivableSnapshotStoreFactory;
import io.camunda.zeebe.snapshots.ReceivedSnapshot;
import io.camunda.zeebe.util.FileUtil;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class SystemPartitionFactory {
  public Optional<RaftPartition> start(
      final Path rootPath,
      final ClusterMembershipService membershipService,
      final ClusterCommunicationService communicationService) {
    final var partitionGroup = buildRaftPartitionGroup(rootPath, new NoopSnapshotFactory());
    final var partitionService =
        new DefaultPartitionService(membershipService, communicationService, partitionGroup);
    partitionService.start();
    final var raftPartition = (RaftPartition) partitionService.getPartitionGroup().getPartition(1);
    if (raftPartition.isRunningOnThisBroker(membershipService.getLocalMember().id())) {
      return Optional.of(raftPartition);
    } else {
      return Optional.empty();
    }
  }

  private RaftPartitionGroup buildRaftPartitionGroup(
      final Path rootPath, final ReceivableSnapshotStoreFactory snapshotStoreFactory) {
    try {
      FileUtil.ensureDirectoryExists(rootPath);
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed to create data directory", e);
    }
    final var raftDataDirectory = rootPath.resolve("system-partition");

    try {
      FileUtil.ensureDirectoryExists(raftDataDirectory);
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed to create Raft data directory", e);
    }

    final Builder partitionGroupBuilder =
        RaftPartitionGroup.builder(PartitionManagerImpl.GROUP_NAME)
            .withNumPartitions(1)
            .withPartitionSize(3)
            .withMembers(getRaftGroupMembers())
            .withDataDirectory(raftDataDirectory.toFile())
            .withSnapshotStoreFactory(snapshotStoreFactory);

    return partitionGroupBuilder.build();
  }

  private List<String> getRaftGroupMembers() {
    final int clusterSize = 3; // system partition is always replicated to first 3 nodes
    final List<String> members = new ArrayList<>();
    for (int i = 0; i < clusterSize; i++) {
      members.add(Integer.toString(i));
    }
    return members;
  }

  private class NoopRecievableSnapshotStore implements ReceivableSnapshotStore {

    @Override
    public boolean hasSnapshotId(final String id) {
      return false;
    }

    @Override
    public Optional<PersistedSnapshot> getLatestSnapshot() {
      return Optional.empty();
    }

    @Override
    public ActorFuture<Set<PersistedSnapshot>> getAvailableSnapshots() {
      return CompletableActorFuture.completed(Set.of());
    }

    @Override
    public ActorFuture<Void> purgePendingSnapshots() {
      return CompletableActorFuture.completed(null);
    }

    @Override
    public ActorFuture<Boolean> addSnapshotListener(final PersistedSnapshotListener listener) {
      return CompletableActorFuture.completed(true);
    }

    @Override
    public ActorFuture<Boolean> removeSnapshotListener(final PersistedSnapshotListener listener) {
      return CompletableActorFuture.completed(true);
    }

    @Override
    public long getCurrentSnapshotIndex() {
      return 0;
    }

    @Override
    public ActorFuture<Void> delete() {
      return CompletableActorFuture.completed(null);
    }

    @Override
    public Path getPath() {
      return null;
    }

    @Override
    public ActorFuture<Void> copySnapshot(
        final PersistedSnapshot snapshot, final Path targetDirectory) {
      return null;
    }

    @Override
    public ReceivedSnapshot newReceivedSnapshot(final String snapshotId) {
      return null;
    }

    @Override
    public void close() {}
  }

  private class NoopSnapshotFactory implements ReceivableSnapshotStoreFactory {

    @Override
    public ReceivableSnapshotStore createReceivableSnapshotStore(
        final Path directory, final int partitionId) {
      return new NoopRecievableSnapshotStore();
    }
  }
}
