/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.system.partitions.impl.steps;

import io.atomix.raft.RaftServer.Role;
import io.camunda.zeebe.backup.BackupActor;
import io.camunda.zeebe.backup.LocalFileSystemBackupStore;
import io.camunda.zeebe.broker.system.partitions.PartitionTransitionContext;
import io.camunda.zeebe.broker.system.partitions.PartitionTransitionStep;
import io.camunda.zeebe.util.sched.future.ActorFuture;
import io.camunda.zeebe.util.sched.future.CompletableActorFuture;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

public final class BackupStorePartitionTransitionStep implements PartitionTransitionStep {
  public static final Path BACKUP_ROOT_DIRECTORY = Paths.get("/tmp/", UUID.randomUUID().toString());

  @Override
  public ActorFuture<Void> prepareTransition(
      final PartitionTransitionContext context, final long term, final Role targetRole) {

    final var backupActor = context.getBackupActor();
    if (backupActor != null
        && (shouldInstallOnTransition(targetRole, context.getCurrentRole())
            || targetRole == Role.INACTIVE)) {
      context.setBackupActor(null);
      return backupActor.closeAsync();
    }
    return CompletableActorFuture.completed(null);
  }

  @Override
  public ActorFuture<Void> transitionTo(
      final PartitionTransitionContext context, final long term, final Role targetRole) {
    if (shouldInstallOnTransition(targetRole, context.getCurrentRole())
        || (context.getBackupActor() == null && targetRole != Role.INACTIVE)) {

      try {
        Files.createDirectories(BACKUP_ROOT_DIRECTORY);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      final LocalFileSystemBackupStore localFileSystemBackupStore =
          new LocalFileSystemBackupStore(
              BACKUP_ROOT_DIRECTORY, context.getNodeId(), context.getPartitionId());
      context.getActorSchedulingService().submitActor(localFileSystemBackupStore);
      final BackupActor backupActor =
          new BackupActor(
              localFileSystemBackupStore,
              context.getConstructableSnapshotStore(),
              context.getLogCompactor());
      context.setBackupActor(backupActor);
      return context.getActorSchedulingService().submitActor(backupActor);
    }

    return CompletableActorFuture.completed(null);
  }

  @Override
  public String getName() {
    return "BackupActor";
  }

  private boolean shouldInstallOnTransition(final Role newRole, final Role currentRole) {
    return newRole == Role.LEADER
        || (newRole == Role.FOLLOWER && currentRole != Role.CANDIDATE)
        || (newRole == Role.CANDIDATE && currentRole != Role.FOLLOWER);
  }
}
