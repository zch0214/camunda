/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.state.appliers;

import io.camunda.zeebe.engine.state.TypedEventApplier;
import io.camunda.zeebe.engine.state.mutable.MutableCheckpointState;
import io.camunda.zeebe.protocol.impl.record.value.checkpoint.CheckpointRecord;
import io.camunda.zeebe.protocol.record.intent.CheckpointIntent;

public class CheckpointCreatedApplier
    implements TypedEventApplier<CheckpointIntent, CheckpointRecord> {

  private final MutableCheckpointState checkpointState;

  public CheckpointCreatedApplier(final MutableCheckpointState state) {
    checkpointState = state;
  }

  @Override
  public void applyState(final long processDefinitionKey, final CheckpointRecord value) {
    checkpointState.storeCheckpointId(value.getCheckpointId());
    checkpointState.storeCheckpointPosition(value.getCheckpointPosition());
  }
}
