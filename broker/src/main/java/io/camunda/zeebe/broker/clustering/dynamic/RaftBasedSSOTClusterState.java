/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic;

import io.atomix.raft.RaftCommitListener;
import io.atomix.raft.partition.RaftPartition;
import io.atomix.raft.storage.log.IndexedRaftLogEntry;
import io.atomix.raft.zeebe.ZeebeLogAppender.AppendListener;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

// Running only on system partition's leader
public class RaftBasedSSOTClusterState implements SSOTClusterState, RaftCommitListener {
  private RaftPartition partition;
  private Cluster cluster;

  void initialize() {
    final var reader = partition.getServer().openReader();
    // Built latest state by reading raft entries
  }

  @Override
  public CompletableFuture<Cluster> getClusterState() {
    return CompletableFuture.completedFuture(cluster);
  }

  public void setClusterState(final Cluster cluster) {
    // TODO
    final ByteBuffer data = ByteBuffer.wrap(new byte[1]);
    partition
        .getServer()
        .getAppender()
        .ifPresent(
            a ->
                a.appendEntry(
                    1,
                    1,
                    data,
                    new AppendListener() {
                      @Override
                      public void onCommit(final IndexedRaftLogEntry indexed) {
                        // update local field cluster
                      }
                    }));
  }

  @Override
  public void onCommit(final long index) {
    // Read latest entry and built latest state
  }
}
