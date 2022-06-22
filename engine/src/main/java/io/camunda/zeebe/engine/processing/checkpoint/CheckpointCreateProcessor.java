/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.checkpoint;

import io.camunda.zeebe.backup.BackupActor;
import io.camunda.zeebe.engine.processing.streamprocessor.CommandProcessor;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecord;
import io.camunda.zeebe.engine.processing.streamprocessor.sideeffect.SideEffectProducer;
import io.camunda.zeebe.engine.state.immutable.LastCheckpointState;
import io.camunda.zeebe.protocol.impl.record.value.checkpoint.CheckpointRecord;
import io.camunda.zeebe.protocol.record.intent.CheckpointIntent;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckpointCreateProcessor implements CommandProcessor<CheckpointRecord> {

  private static final Logger LOG = LoggerFactory.getLogger("Checkpoint");
  private final LastCheckpointState state;
  private final BackupActor backupActor;

  public CheckpointCreateProcessor(final LastCheckpointState state, final BackupActor backupActor) {
    this.state = state;
    this.backupActor = backupActor;
  }

  @Override
  public boolean onCommand(
      final TypedRecord<CheckpointRecord> command,
      final CommandControl<CheckpointRecord> commandControl,
      final Consumer<SideEffectProducer> sideEffect) {
    final long checkpointId = command.getValue().getCheckpointId();
    if (state.getCheckpointId() < checkpointId) {
      final long checkpointPosition = command.getPosition();
      final var updatedValue = command.getValue().setCheckpointPosition(checkpointPosition);
      commandControl.accept(CheckpointIntent.CREATED, updatedValue);
      backupActor.takeBackup(checkpointId, checkpointPosition);

      LOG.info("Creating checkpoint {} at position {}", checkpointId, checkpointPosition);
    } else {
      final var updatedValue =
          command
              .getValue()
              .setCheckpointId(state.getCheckpointId())
              .setCheckpointPosition(state.getCheckpointPosition());
      commandControl.accept(CheckpointIntent.IGNORED, updatedValue);
      LOG.info(
          "Ignoring checkpoint command for id {}. Checkpoint {} exists at position {}",
          checkpointId,
          state.getCheckpointId(),
          state.getCheckpointPosition());
    }

    return true;
  }
}
