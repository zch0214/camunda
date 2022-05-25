/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.backup;

import io.camunda.zeebe.util.sched.Actor;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class BackupActor extends Actor {

  private final LocalFileSystemBackupStore backupStore;

  public BackupActor(final LocalFileSystemBackupStore backupStore) {
    this.backupStore = backupStore;
  }

  public void takeBackup(final long checkpointId, final long checkpointPosition) {
    actor.run(
        () -> {
          final Path snapshotDirectory = null; // TODO
          final List<Path> segmentFiles = null; // TODO
          startBackup(checkpointId, checkpointPosition, snapshotDirectory, segmentFiles);
        });
  }

  private void startBackup(
      final long checkpointId,
      final long checkpointPosition,
      final Path snapshotDirectory,
      final List<Path> segmentFiles) {

    final Backup backup = new Backup(checkpointId, checkpointPosition);
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
      localFileSystemBackup.markAsCompleted();
    } catch (final IOException e) {
      // TODO
    }
  }

  private void onBackupFailed(
      final LocalFileSystemBackup localFileSystemBackup, final Throwable error) {
    try {
      localFileSystemBackup.markAsFailed();
    } catch (final IOException e) {
      // TODO:
    }
  }
}
