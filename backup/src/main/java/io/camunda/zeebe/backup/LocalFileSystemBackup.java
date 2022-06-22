/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.backup;

import io.camunda.zeebe.util.sched.ActorControl;
import io.camunda.zeebe.util.sched.future.ActorFuture;
import io.camunda.zeebe.util.sched.future.CompletableActorFuture;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

// TODO: Write checkpointPosition to the backup directory
public class LocalFileSystemBackup implements Backup {

  private final ActorControl actor;
  private final BackupMetaData backup;
  private final Path backupDirectory;

  public LocalFileSystemBackup(
      final Path backupDirectory, final BackupMetaData backup, final ActorControl actor) {
    this.backupDirectory = backupDirectory;
    this.backup = backup;
    this.actor = actor;
  }

  @Override
  public ActorFuture<Void> backupSnapshot(final Path snapshotDirectory) {
    final CompletableActorFuture<Void> snapshotBackedUp = new CompletableActorFuture<>();
    actor.run(
        () -> {
          try {
            final var statusFile =
                Paths.get(backupDirectory.toString(), BackupStatus.ONGOING.name());
            if (Files.isDirectory(backupDirectory)
                && backupDirectory.toFile().listFiles().length == 0) {
              // If initializing the backup
              Files.createFile(statusFile);
            }
            copySnapshot(snapshotDirectory, snapshotBackedUp);
          } catch (final IOException e) {
            snapshotBackedUp.completeExceptionally(e);
          }
        });
    return snapshotBackedUp;
  }

  @Override
  public ActorFuture<Void> backupSegments(final List<Path> segmentFiles) {
    final CompletableActorFuture<Void> segmentsBackedUp = new CompletableActorFuture<>();
    actor.run(
        () -> {
          try {
            copyFiles(segmentFiles, Paths.get(backupDirectory.toString(), "segments"));
            segmentsBackedUp.complete(null);
          } catch (final Exception e) {
            segmentsBackedUp.completeExceptionally(e);
          }
        });
    return segmentsBackedUp;
  }

  @Override
  public void markAsCompleted() throws IOException {
    final var statusFile = Paths.get(backupDirectory.toString(), BackupStatus.ONGOING.name());
    Files.move(statusFile, Paths.get(backupDirectory.toString(), BackupStatus.COMPLETED.name()));
  }

  @Override
  public void markAsFailed() throws IOException {
    final var statusFile = Paths.get(backupDirectory.toString(), BackupStatus.ONGOING.name());
    Files.move(statusFile, Paths.get(backupDirectory.toString(), BackupStatus.FAILED.name()));
  }

  @Override
  public ActorFuture<BackupStatus> getStatus() {
    final var possibleStatus = Arrays.stream(BackupStatus.values()).map(Enum::name).toList();
    if (!backupDirectory.toFile().exists()) {
      return CompletableActorFuture.completed(BackupStatus.NOT_FOUND);
    }
    final String[] backupContents = backupDirectory.toFile().list();
    if (backupContents == null) {
      return CompletableActorFuture.completed(BackupStatus.NOT_FOUND);
    }
    final var status =
        Arrays.stream(backupContents)
            .filter(possibleStatus::contains)
            .map(BackupStatus::valueOf)
            .findFirst();
    final var backupStatus = status.orElse(BackupStatus.NOT_FOUND);
    return CompletableActorFuture.completed(backupStatus);
  }

  private void copySnapshot(
      final Path snapshotDirectory, final CompletableActorFuture<Void> snapshotBackedUp) {
    final List<Path> snapshotFiles =
        Arrays.stream(Objects.requireNonNull(snapshotDirectory.toFile().listFiles()))
            .map(File::toPath)
            .toList();
    try {
      copyFiles(
          snapshotFiles,
          Paths.get(
              backupDirectory.toString(), "snapshot", snapshotDirectory.getFileName().toString()));
      snapshotBackedUp.complete(null);
    } catch (final Exception e) {
      snapshotBackedUp.completeExceptionally(e);
    }
  }

  public static void copyFiles(final List<Path> sourceFiles, final Path destinationDirectory)
      throws IOException {
    // TODO: Split copying files into multiple actor task to not block the actor for a long time.
    for (final Path source : sourceFiles) {
      final Path destination =
          Paths.get(destinationDirectory.toString(), source.getFileName().toString());
      Files.copy(source, destination);
    }
  }
}
