/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.snapshots;

import java.nio.file.Path;

public interface CopyableSnapshotStore {

  // Copy snapshot files and its metadata (like checksum).
  void copySnapshotTo(final PersistedSnapshot snapshot, final Path destinationDirectory)
      throws Exception;

  void restoreSnapshotFrom(final Path sourceDirectory) throws Exception;
}
