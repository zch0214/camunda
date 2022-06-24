/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.backup;

import io.camunda.zeebe.snapshots.PersistedSnapshot;
import io.camunda.zeebe.util.sched.future.ActorFuture;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface Backup {

  ActorFuture<Void> backupSnapshot(PersistedSnapshot snapshot);

  ActorFuture<Void> backupSegments(List<Path> segmentFiles);

  void markAsCompleted() throws IOException;

  void markAsFailed() throws IOException;

  ActorFuture<BackupStatus> getStatus();

  void restore(Path dataDirectory) throws Exception;
}
