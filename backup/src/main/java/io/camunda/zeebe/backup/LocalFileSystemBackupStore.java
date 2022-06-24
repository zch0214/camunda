/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.backup;

import io.camunda.zeebe.snapshots.CopyableSnapshotStore;
import io.camunda.zeebe.util.sched.Actor;
import io.camunda.zeebe.util.sched.future.ActorFuture;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LocalFileSystemBackupStore extends Actor implements BackupStore {

  private final Path backupRootDirectory;
  private final int brokerNodeId;
  private final int partitionId;
  private final CopyableSnapshotStore snapshotStore;

  public LocalFileSystemBackupStore(
      final Path backupRootDirectory,
      final int brokerNodeId,
      final int partitionId,
      final CopyableSnapshotStore snapshotStore) {
    this.backupRootDirectory = backupRootDirectory;
    this.brokerNodeId = brokerNodeId;
    this.partitionId = partitionId;
    this.snapshotStore = snapshotStore;
  }

  @Override
  public LocalFileSystemBackup newBackup(final BackupMetaData backup) throws IOException {
    // backuproot/checkpointId/partitionId/brokerId/
    final var partitionBackupDirectory = getPath(backup.checkpointId());
    Files.createDirectories(partitionBackupDirectory);
    return new LocalFileSystemBackup(partitionBackupDirectory, backup, actor, snapshotStore);
  }

  @Override
  public ActorFuture<BackupStatus> getStatus(final BackupMetaData backupMetadata) {
    final var partitionBackupDirectory = getPath(backupMetadata.checkpointId());
    final var backup =
        new LocalFileSystemBackup(partitionBackupDirectory, backupMetadata, actor, snapshotStore);
    return backup.getStatus();
  }

  public Path getPath(final long checkpointId) {
    return Paths.get(
        backupRootDirectory.toString(),
        String.valueOf(checkpointId),
        String.valueOf(partitionId),
        String.valueOf(brokerNodeId));
  }

  public Backup loadBackup(final long checkpointId) {
    final var partitionBackupDirectory = getPath(checkpointId);
    return LocalFileSystemBackup.loadBackup(
        partitionBackupDirectory, checkpointId, actor, snapshotStore);
  }
}
