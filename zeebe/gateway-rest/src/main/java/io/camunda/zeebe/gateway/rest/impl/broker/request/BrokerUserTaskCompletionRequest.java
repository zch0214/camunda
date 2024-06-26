/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.gateway.rest.impl.broker.request;

import io.camunda.zeebe.broker.client.api.dto.BrokerExecuteCommand;
import io.camunda.zeebe.protocol.impl.record.value.usertask.UserTaskRecord;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.UserTaskIntent;
import org.agrona.DirectBuffer;

public class BrokerUserTaskCompletionRequest extends BrokerExecuteCommand<UserTaskRecord> {

  private final UserTaskRecord requestDto = new UserTaskRecord();

  public BrokerUserTaskCompletionRequest(
      final long key, final DirectBuffer variables, final String action) {
    super(ValueType.USER_TASK, UserTaskIntent.COMPLETE);
    requestDto.setUserTaskKey(key).setVariables(variables).setAction(action);
    request.setKey(key);
  }

  @Override
  public UserTaskRecord getRequestWriter() {
    return requestDto;
  }

  @Override
  protected UserTaskRecord toResponseDto(final DirectBuffer buffer) {
    final var responseDto = new UserTaskRecord();
    responseDto.wrap(buffer);
    return responseDto;
  }
}
