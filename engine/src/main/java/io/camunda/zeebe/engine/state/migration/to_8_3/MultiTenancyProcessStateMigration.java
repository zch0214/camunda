/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.state.migration.to_8_3;

import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.engine.state.immutable.ProcessingState;
import io.camunda.zeebe.engine.state.migration.MigrationTask;
import io.camunda.zeebe.engine.state.mutable.MutableProcessingState;
import io.camunda.zeebe.protocol.ZbColumnFamilies;

public final class MultiTenancyProcessStateMigration implements MigrationTask {

  @Override
  public String getIdentifier() {
    return getClass().getSimpleName();
  }

  @Override
  public boolean needsToRun(final ProcessingState processingState) {
    return hasDeployedProcessesInDeprecatedCF(processingState);
  }

  @Override
  public void runMigration(
      final MutableProcessingState processingState, final TransactionContext txnContext) {
    final var migrationState = processingState.getMigrationState();
    migrationState.migrateProcessStateForMultiTenancy(txnContext);
  }

  private static boolean hasDeployedProcessesInDeprecatedCF(final ProcessingState processingState) {
    return !processingState.isEmpty(ZbColumnFamilies.DEPRECATED_PROCESS_CACHE);
  }
}
