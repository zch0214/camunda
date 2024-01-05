/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.gateway.rest;

import static io.camunda.zeebe.gateway.RequestMapper.ensureJsonSet;

import io.camunda.zeebe.gateway.impl.broker.request.BrokerUserTaskCompleteRequest;

public final class RestRequestMapper {
  private RestRequestMapper() {}

  public static BrokerUserTaskCompleteRequest toCompleteUserTaskRequest(
      final long userTaskKey, final String variables) {
    return new BrokerUserTaskCompleteRequest(userTaskKey, ensureJsonSet(variables));
  }
}
