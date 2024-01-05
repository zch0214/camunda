/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */ package io.camunda.zeebe.gateway.impl.broker.request;

import io.camunda.zeebe.protocol.impl.record.value.usertask.UserTaskRecord;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.UserTaskIntent;
import org.agrona.DirectBuffer;

public class BrokerUserTaskCompleteRequest extends BrokerExecuteCommand<UserTaskRecord> {

  private final UserTaskRecord requestDto = new UserTaskRecord();

  public BrokerUserTaskCompleteRequest(final long key, final DirectBuffer variables) {
    super(ValueType.USER_TASK, UserTaskIntent.COMPLETE);
    request.setKey(key);
    requestDto.setVariables(variables);
  }

  @Override
  public UserTaskRecord getRequestWriter() {
    return requestDto;
  }

  @Override
  protected UserTaskRecord toResponseDto(final DirectBuffer buffer) {
    final UserTaskRecord responseDto = new UserTaskRecord();
    responseDto.wrap(buffer);
    return responseDto;
  }
}
