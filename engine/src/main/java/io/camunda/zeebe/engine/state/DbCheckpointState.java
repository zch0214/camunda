/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.state;

import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.db.ZeebeDb;
import io.camunda.zeebe.engine.state.mutable.MutableCheckpointState;

public class DbCheckpointState implements MutableCheckpointState {

  // TODO: Store in zeebedb
  long checkpointId = 0;
  long checkpointPosition = 0;

  public DbCheckpointState(
      final ZeebeDb<ZbColumnFamilies> zeebeDb, final TransactionContext transactionContext) {}

  @Override
  public long getCheckpointId() {
    return checkpointId;
  }

  @Override
  public long getCheckpointPosition() {
    return checkpointPosition;
  }

  @Override
  public void storeCheckpointId(final long checkpointId) {
    this.checkpointId = checkpointId;
  }

  @Override
  public void storeCheckpointPosition(final long checkpointPosition) {
    this.checkpointPosition = checkpointPosition;
  }
}
