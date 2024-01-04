/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */ package io.camunda.zeebe.gateway.rest;

import java.util.Map;
import org.openapitools.api.UserTasksApi;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("${openapi.zeebeREST.base-path:/api/v1}")
public class UserTaskApiImpl implements UserTasksApi {

  @Override
  public ResponseEntity<Void> userTasksUserTaskKeyCompletePost(
      final Long userTaskKey, final Object body) {

    final Map<String, Object> variables = (Map) body;
    return sendRequest(userTaskKey, variables);
  }

  public ResponseEntity<Void> sendRequest(
      final Long userTaskKey, final Map<String, Object> variables) {
    return new ResponseEntity<>(HttpStatus.NO_CONTENT);
  }
}
