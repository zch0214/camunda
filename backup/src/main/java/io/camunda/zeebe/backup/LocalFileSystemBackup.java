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

public class LocalFileSystemBackup {

  private static final String ongoingBackupFile = "-%d-ongoing";
  private static final String completedBackupFile = "-%d-completed";
  private static final String failedBackupFile = "-%d-failed";
  private final ActorControl actor;
  private final Backup backup;
  private final Path backupRootDirectory;
  private final Path backupDirectory;

  public LocalFileSystemBackup(
      final Path backupRootDirectory, final Backup backup, final ActorControl actor)
      throws IOException {
    this.backupRootDirectory = backupRootDirectory;
    backupDirectory =
        Paths.get(
            backupRootDirectory.toString(),
            String.format(ongoingBackupFile, backup.checkpointId()));
    Files.createDirectory(backupDirectory);
    this.backup = backup;
    this.actor = actor;
  }

  public ActorFuture<Void> backupSnapshot(final Path snapshotDirectory) {
    final CompletableActorFuture<Void> snapshotBackedUp = new CompletableActorFuture<>();
    actor.run(() -> copySnapshot(snapshotDirectory, snapshotBackedUp));
    return snapshotBackedUp;
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
          Paths.get(backupDirectory.toString(), snapshotDirectory.getFileName().toString()));
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

  public ActorFuture<Void> backupSegments(final List<Path> segmentFiles) {
    final CompletableActorFuture<Void> segmentsBackedUp = new CompletableActorFuture<>();
    actor.run(
        () -> {
          try {
            copyFiles(segmentFiles, backupDirectory);
            segmentsBackedUp.complete(null);
          } catch (final Exception e) {
            segmentsBackedUp.completeExceptionally(e);
          }
        });
    return segmentsBackedUp;
  }

  public void markAsCompleted() throws IOException {
    final Path backupCompletedDirectory =
        Paths.get(
            backupRootDirectory.toString(),
            String.format(completedBackupFile, backup.checkpointId()));
    Files.move(backupDirectory, backupCompletedDirectory);
  }

  public void markAsFailed() throws IOException {
    final Path backupCompletedDirectory =
        Paths.get(
            backupRootDirectory.toString(), String.format(failedBackupFile, backup.checkpointId()));

    Files.move(backupDirectory, backupCompletedDirectory);
  }
}
