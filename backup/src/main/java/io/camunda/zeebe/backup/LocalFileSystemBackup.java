/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.backup;

import io.camunda.zeebe.snapshots.CopyableSnapshotStore;
import io.camunda.zeebe.snapshots.PersistedSnapshot;
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

// TODO: Write checkpointPosition to the backup directory
public class LocalFileSystemBackup implements Backup {

  private static final String SNAPSHOT_DIRECTORY_NAME = "snapshot";
  private static final String SEGMENTS_DIRECTORY_NAME = "segments";
  private final ActorControl actor;
  private final BackupMetaData backup;
  private final Path backupDirectory;

  private final CopyableSnapshotStore snapshotStore;

  public LocalFileSystemBackup(
      final Path backupDirectory,
      final BackupMetaData backup,
      final ActorControl actor,
      final CopyableSnapshotStore snapshotStore) {
    this.backupDirectory = backupDirectory;
    this.backup = backup;
    this.actor = actor;
    this.snapshotStore = snapshotStore;
  }

  public static Backup loadBackup(
      final Path backupDirectory,
      final long checkpointId,
      final ActorControl actor,
      final CopyableSnapshotStore snapshotStore) {
    return new LocalFileSystemBackup(
        backupDirectory, new BackupMetaData(checkpointId, -1), actor, snapshotStore);
  }

  @Override
  public ActorFuture<Void> backupSnapshot(final PersistedSnapshot snapshot) {
    final CompletableActorFuture<Void> future = new CompletableActorFuture<>();
    actor.submit(
        () -> {
          try {
            final Path destinationDirectory =
                Paths.get(backupDirectory.toString(), SNAPSHOT_DIRECTORY_NAME);
            Files.createDirectories(destinationDirectory);
            snapshotStore.copySnapshotTo(snapshot, destinationDirectory);
            future.complete(null);
          } catch (final Exception e) {
            future.completeExceptionally(e);
          }
        });
    return future;
  }

  @Override
  public ActorFuture<Void> backupSegments(final List<Path> segmentFiles) {
    final CompletableActorFuture<Void> segmentsBackedUp = new CompletableActorFuture<>();
    actor.submit(
        () -> {
          try {
            copyFiles(segmentFiles, Paths.get(backupDirectory.toString(), SEGMENTS_DIRECTORY_NAME));
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

  @Override
  public void restore(final Path dataDirectory) throws Exception {

    final List<Path> segmentFiles =
        Arrays.stream(
                Paths.get(backupDirectory.toString(), SEGMENTS_DIRECTORY_NAME).toFile().listFiles())
            .map(File::toPath)
            .toList();
    copyFiles(segmentFiles, dataDirectory);

    // copy snapshot to dataDirectory
    final Path sourceDirectory = Paths.get(backupDirectory.toString(), SNAPSHOT_DIRECTORY_NAME);
    snapshotStore.restoreSnapshotFrom(sourceDirectory);
  }

  public static void copyFiles(final List<Path> sourceFiles, final Path destinationDirectory)
      throws IOException {
    // TODO: Split copying files into multiple actor task to not block the actor for a long time.
    Files.createDirectories(destinationDirectory); // create if not exists
    for (final Path source : sourceFiles) {
      final Path destination =
          Paths.get(destinationDirectory.toString(), source.getFileName().toString());
      Files.copy(source, destination);
    }
  }
}
