/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.logstreams;

import io.atomix.raft.partition.impl.RaftPartitionServer;
import io.camunda.zeebe.backup.LogCompactor;
import io.camunda.zeebe.broker.Loggers;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public final class AtomixLogCompactor implements LogCompactor {
  private final RaftPartitionServer partitionServer;

  // TODO: Find a better way to handle concurrent backups enabling and disabling concurrently. If
  // there are back to back backups, we may never compact. Instead, we can compact segments that
  // were included in already completed backups.
  private final AtomicInteger disableCompaction = new AtomicInteger(0);

  public AtomixLogCompactor(final RaftPartitionServer partitionServer) {
    this.partitionServer = partitionServer;
  }

  /**
   * Sets the compactable index on the Atomix side and triggers compaction. On failure will log the
   * error but will return a "successful" future - arguable if this is desired behavior.
   *
   * @param compactionBound the upper index compaction bound
   * @return a future which is completed after compaction is finished
   */
  @Override
  public CompletableFuture<Void> compactLog(final long compactionBound) {
    if (disableCompaction.get() == 0) {
      Loggers.DELETION_SERVICE.debug("Scheduling log compaction up to index {}", compactionBound);
      partitionServer.setCompactableIndex(compactionBound);
      return partitionServer.snapshot();
    }
    Loggers.DELETION_SERVICE.debug("Compaction is disabled");
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void disableCompaction() {
    disableCompaction.getAndIncrement();
  }

  @Override
  public void enableCompaction() {
    // TODO: do the previously skipped compaction
    disableCompaction.getAndDecrement();
  }
}
