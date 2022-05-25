/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.backup;

import io.camunda.zeebe.util.sched.Actor;
import io.camunda.zeebe.util.sched.future.ActorFuture;
import io.camunda.zeebe.util.sched.future.CompletableActorFuture;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LocalFileSystemBackupStore extends Actor {

  private final Path backupRootDirectory;
  private final int brokerNodeId;
  private final int partitionId;

  private final Path partitionBackupRootDirecotry;

  public LocalFileSystemBackupStore(
      final Path backupRootDirectory, final int brokerNodeId, final int partitionId)
      throws IOException {
    this.backupRootDirectory = backupRootDirectory;
    this.brokerNodeId = brokerNodeId;
    this.partitionId = partitionId;
    partitionBackupRootDirecotry =
        Paths.get(
            backupRootDirectory.toString(),
            String.valueOf(partitionId),
            String.valueOf(brokerNodeId));
    Files.createDirectory(partitionBackupRootDirecotry);
  }

  public LocalFileSystemBackup createBackup(final Backup backup) throws IOException {
    return new LocalFileSystemBackup(partitionBackupRootDirecotry, backup, actor);
  }

  public ActorFuture<BackupStatus> getStatus(final Backup backup) {
    // TODO : Check the directory to find the backup
    return CompletableActorFuture.completed(BackupStatus.NOT_FOUND);
  }
}
