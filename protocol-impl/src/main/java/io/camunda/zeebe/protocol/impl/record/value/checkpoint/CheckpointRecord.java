/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.protocol.impl.record.value.checkpoint;

import io.camunda.zeebe.msgpack.property.LongProperty;
import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue;

public class CheckpointRecord extends UnifiedRecordValue {

  private final LongProperty checkpointIdProperty = new LongProperty("checkpointId", -1);

  public CheckpointRecord() {
    declareProperty(checkpointIdProperty);
  }

  public long getCheckpointId() {
    return checkpointIdProperty.getValue();
  }

  public CheckpointRecord setCheckpointId(final long checkpointId) {
    checkpointIdProperty.setValue(checkpointId);
    return this;
  }
}
