/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.backup;

import java.util.concurrent.CompletableFuture;

// TODO: This interface shouldnt be in this package
public interface LogCompactor {
  CompletableFuture<Void> compactLog(final long compactionBound);

  void disableCompaction();

  void enableCompaction();
}
