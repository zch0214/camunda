/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.backup;

import io.camunda.zeebe.snapshots.PersistedSnapshotStore;
import io.camunda.zeebe.util.sched.Actor;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class BackupActor extends Actor {

  private final LocalFileSystemBackupStore backupStore;

  private final PersistedSnapshotStore snapshotStore;
  private final LogCompactor logCompactor;

  public BackupActor(
      final LocalFileSystemBackupStore backupStore,
      final PersistedSnapshotStore snapshotStore,
      final LogCompactor logCompactor) {
    this.backupStore = backupStore;
    this.snapshotStore = snapshotStore;
    this.logCompactor = logCompactor;
  }

  public void takeBackup(final long checkpointId, final long checkpointPosition) {
    actor.run(
        () -> {
          logCompactor.disableCompaction();
          final var snapshotFuture = snapshotStore.lockLatestSnapshot();
          actor.runOnCompletion(
              snapshotFuture,
              (snapshot, error) -> {
                if (error == null) {
                  if (snapshot.getSnapshotId().getProcessedPosition() < checkpointPosition) {
                    final Path snapshotDirectory = snapshot.getPath();
                    final List<Path> segmentFiles = null; // TODO, get the current segment files
                    startBackup(checkpointId, checkpointPosition, snapshotDirectory, segmentFiles);
                  } else {
                    // TODO: log error
                    // mark backup as failed
                    snapshotStore.unlockSnapshot(snapshot);
                    logCompactor.enableCompaction();
                  }
                }
              });
        });
  }

  private void startBackup(
      final long checkpointId,
      final long checkpointPosition,
      final Path snapshotDirectory,
      final List<Path> segmentFiles) {

    final Backup backup = new Backup(checkpointId);
    final LocalFileSystemBackup localFileSystemBackup;
    try {
      localFileSystemBackup = backupStore.createBackup(backup);

      final var snapshotBackedUp = localFileSystemBackup.backupSnapshot(snapshotDirectory);
      final var segmentsBackedUp = localFileSystemBackup.backupSegments(segmentFiles);
      actor.runOnCompletion(
          List.of(snapshotBackedUp, segmentsBackedUp),
          error -> {
            if (error != null) {
              onBackupFailed(localFileSystemBackup, error);
            } else {
              onBackupCompleted(localFileSystemBackup);
            }
          });

    } catch (final IOException e) {
      // TODO: log
    }
  }

  private void onBackupCompleted(final LocalFileSystemBackup localFileSystemBackup) {

    try {
      logCompactor.enableCompaction();
      localFileSystemBackup.markAsCompleted();
    } catch (final IOException e) {
      // TODO
    }
  }

  private void onBackupFailed(
      final LocalFileSystemBackup localFileSystemBackup, final Throwable error) {
    try {
      logCompactor.enableCompaction();
      localFileSystemBackup.markAsFailed();
    } catch (final IOException e) {
      // TODO:
    }
  }
}
