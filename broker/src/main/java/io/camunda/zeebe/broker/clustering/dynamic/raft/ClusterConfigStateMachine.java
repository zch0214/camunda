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
import io.atomix.raft.storage.log.RaftLogReader;
import io.atomix.raft.storage.log.entry.SerializedApplicationEntry;
import io.atomix.raft.zeebe.ZeebeLogAppender.AppendListener;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster;
import io.camunda.zeebe.util.buffer.BufferUtil;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public class ClusterConfigStateMachine implements RaftCommitListener, AppendListener {

  private final ScheduledExecutorService executorService;
  private final RaftPartition raftPartition;
  private final RaftLogReader reader;
  private Cluster cluster;
  private long position;

  public ClusterConfigStateMachine(
      final ScheduledExecutorService executorService, final RaftPartition raftPartition) {
    this.executorService = executorService;
    this.raftPartition = raftPartition;
    reader = raftPartition.getServer().openReader();
  }

  @Override
  public void onCommit(final long indexIgnored) {
    executorService.execute(
        () -> {
          while (reader.hasNext()) {
            final var nextEntry = reader.next();
            if (nextEntry.isApplicationEntry()) {
              final var encodedConfigEntry =
                  ((SerializedApplicationEntry) nextEntry.getApplicationEntry()).data();
              final var encodedBytes = BufferUtil.bufferAsArray(encodedConfigEntry);
              cluster = Cluster.decode(encodedBytes);
            }
          }
        });
  }

  private void snapshot() {
    // TODO: snapshotting and compaction
  }

  CompletableFuture<Cluster> getCluster() {
    final CompletableFuture<Cluster> result = new CompletableFuture<>();
    executorService.execute(() -> result.complete(cluster));
    return result;
  }

  public void setCluster(final Cluster cluster) {
    executorService.execute(
        () -> {
          position++;
          final var data = ByteBuffer.wrap(cluster.encodeAsBytes());
          raftPartition
              .getServer()
              .getAppender()
              .ifPresent(appender -> appender.appendEntry(position, position, data, this));
        });
  }

  public void start() {
    onCommit(0);
  }
}
