/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic.raft;

import io.atomix.raft.RaftCommitListener;
import io.atomix.raft.partition.RaftPartition;
import io.atomix.raft.storage.log.IndexedRaftLogEntry;
import io.atomix.raft.storage.log.RaftLogReader;
import io.atomix.raft.storage.log.entry.ApplicationEntry;
import io.atomix.raft.storage.log.entry.SerializedApplicationEntry;
import io.atomix.raft.zeebe.ZeebeLogAppender;
import io.atomix.raft.zeebe.ZeebeLogAppender.AppendListener;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster;
import io.camunda.zeebe.util.buffer.BufferUtil;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.UnaryOperator;

/**
 * State machine for applying raft entries. To keep it simple, we write the entire cluster state in
 * one entry, instead of update operations. This poses a challenge, on how to serialize an update.
 * This currently handled in this class itself by keeping track of last uncommitted entry, and
 * executing update based on last uncommitted entry.
 */
public class ClusterConfigStateMachine implements RaftCommitListener, AppendListener {
  // TODO: To ensure updates are done sequentially, use EntryValidator, and keep track of the latest
  // uncommitted entry here
  private final ScheduledExecutorService executorService;
  private final RaftPartition raftPartition;
  private final RaftLogReader reader;

  // Following two are committed data
  private Cluster cluster;
  private ApplicationEntry lastReadEntry;

  // Following two can be uncommitted. Should be cleared when transition to follower and initialize
  // when transition to leader
  private Cluster lastWrittenClusterState; // may be uncommitted
  private ApplicationEntry lastWrittenEntry; // may be uncommitted
  private Optional<ZeebeLogAppender> leaderAppender; // updated for each role transition

  public ClusterConfigStateMachine(
      final ScheduledExecutorService executorService, final RaftPartition raftPartition) {
    this.executorService = executorService;
    this.raftPartition = raftPartition;
    reader = raftPartition.getServer().openReader();
  }

  @Override
  public void onCommit(final long indexIgnored) {
    executorService.execute(this::readNextCommittedEntries);
  }

  private void readNextCommittedEntries() {
    while (reader.hasNext()) {
      final var nextEntry = reader.next();
      if (nextEntry.isApplicationEntry()) {
        lastReadEntry = nextEntry.getApplicationEntry();
        final var encodedConfigEntry = ((SerializedApplicationEntry) lastReadEntry).data();
        final var encodedBytes = BufferUtil.bufferAsArray(encodedConfigEntry);
        cluster = Cluster.decode(encodedBytes);
      }
    }
  }

  private void snapshot() {
    // TODO: snapshotting and compaction, SnapshotReplicationListener, initializing from snapshot
    // etc.
  }

  CompletableFuture<Cluster> getCluster() {
    final CompletableFuture<Cluster> result = new CompletableFuture<>();
    // Return only committed state
    executorService.execute(() -> result.complete(cluster));
    return result;
  }

  public void setCluster(final Cluster newCluster) {
    executorService.execute(
        () -> {
          // merge to ensure concurrent changes between gossip and raft are merged correctly. Only
          // required if we use a combination of gossip + raft. For a solution with only raft, all
          // changes can only be executed by the raft leader.
          final Cluster updatedState = merge(lastWrittenClusterState, newCluster);
          update(updatedState, new AppendListener() {});
        });
  }

  private long getNextPosition() {
    if (lastWrittenEntry == null) {
      return 1;
    }
    return lastWrittenEntry.highestPosition() + 1;
  }

  private void update(final Cluster updatedState, final AppendListener listener) {
    final var data = ByteBuffer.wrap(updatedState.encodeAsBytes());
    final var position = getNextPosition();
    leaderAppender.ifPresent(
        appender ->
            appender.appendEntry(
                position,
                position,
                data,
                new AppendListener() {
                  @Override
                  public void onWrite(final IndexedRaftLogEntry indexed) {
                    lastWrittenEntry = indexed.getApplicationEntry();
                    lastWrittenClusterState = updatedState;
                    listener.onWrite(indexed);
                  }

                  @Override
                  public void onWriteError(final Throwable error) {
                    listener.onWriteError(error);
                  }

                  @Override
                  public void onCommit(final IndexedRaftLogEntry indexed) {
                    listener.onCommit(indexed);
                  }

                  @Override
                  public void onCommitError(
                      final IndexedRaftLogEntry indexed, final Throwable error) {
                    listener.onCommitError(indexed, error);
                  }
                }));
  }

  public CompletableFuture<Cluster> update(final UnaryOperator<Cluster> clusterUpdater) {
    final CompletableFuture<Cluster> updated = new CompletableFuture<>();
    executorService.execute(
        () -> {
          final var newCluster = clusterUpdater.apply(lastWrittenClusterState);
          update(
              newCluster,
              new AppendListener() {
                @Override
                public void onCommit(final IndexedRaftLogEntry indexed) {
                  updated.complete(newCluster);
                }

                @Override
                public void onCommitError(
                    final IndexedRaftLogEntry indexed, final Throwable error) {
                  updated.completeExceptionally(error);
                }
              });
        });
    return updated;
  }

  private Cluster merge(final Cluster currentCluster, final Cluster newCluster) {
    if (currentCluster != null) {
      return currentCluster.merge(newCluster);
    } else {
      return newCluster;
    }
  }

  public void start() {
    onCommit(0);
  }

  public void transitionToLeader() {
    executorService.execute(
        () -> {
          leaderAppender = raftPartition.getServer().getAppender();
          readNextCommittedEntries();
          // same as committed state, because this is guaranteed to be called after initial entry is
          // committed and no new entry is written.
          lastWrittenEntry = lastReadEntry;
          lastWrittenClusterState = cluster;
        });
  }

  public void transitionToFollower() {
    executorService.execute(
        () -> {
          leaderAppender = Optional.empty();
          lastWrittenEntry = null;
          lastWrittenClusterState = null;
        });
  }
}
