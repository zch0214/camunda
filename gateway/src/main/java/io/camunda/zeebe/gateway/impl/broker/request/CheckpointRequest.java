/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.gateway.impl.broker.request;

import io.camunda.zeebe.protocol.impl.record.value.checkpoint.CheckpointRecord;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.CheckpointIntent;
import org.agrona.DirectBuffer;

public final class CheckpointRequest extends BrokerExecuteCommand<CheckpointRecord> {

  private final CheckpointRecord requestDto = new CheckpointRecord();

  public CheckpointRequest(final long checkpointId) {
    super(ValueType.CHECKPOINT, CheckpointIntent.CREATE);
    requestDto.setCheckpointId(checkpointId);
  }

  @Override
  public CheckpointRecord getRequestWriter() {
    return requestDto;
  }

  @Override
  protected CheckpointRecord toResponseDto(final DirectBuffer buffer) {
    final CheckpointRecord responseDto = new CheckpointRecord();
    responseDto.wrap(buffer);
    return responseDto;
  }

  @Override
  public String toString() {
    return "CheckpointRequest{" + "requestDto=" + requestDto + '}';
  }
}
