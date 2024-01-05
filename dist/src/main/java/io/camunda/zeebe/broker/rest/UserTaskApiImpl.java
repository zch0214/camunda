/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */ package io.camunda.zeebe.broker.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.gateway.RestRequestMapper;
import io.camunda.zeebe.gateway.impl.broker.BrokerClient;
import io.camunda.zeebe.gateway.impl.broker.request.BrokerUserTaskCompleteRequest;
import io.camunda.zeebe.gateway.impl.broker.response.BrokerResponse;
import io.camunda.zeebe.protocol.impl.record.value.usertask.UserTaskRecord;
import java.util.concurrent.CompletableFuture;
import org.openapitools.api.UserTasksApi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("${openapi.zeebeREST.base-path:/api/v1}")
public class UserTaskApiImpl implements UserTasksApi {

  final BrokerClient brokerClient;
  final ObjectMapper mapper;

  @Autowired
  public UserTaskApiImpl(final BrokerClient brokerClient, final ObjectMapper mapper) {
    this.brokerClient = brokerClient;
    this.mapper = mapper;
  }

  @Override
  public CompletableFuture<ResponseEntity<Void>> userTasksUserTaskKeyCompletePost(
      final Long userTaskKey, final Object body) {

    final String variables;
    try {
      variables = mapper.writeValueAsString(body);
    } catch (final JsonProcessingException e) {
      return CompletableFuture.completedFuture(new ResponseEntity<>(HttpStatus.BAD_REQUEST));
    }

    return sendRequest(userTaskKey, variables);
  }

  public CompletableFuture<ResponseEntity<Void>> sendRequest(
      final Long userTaskKey, final String variables) {
    final BrokerUserTaskCompleteRequest request =
        RestRequestMapper.toCompleteUserTaskRequest(userTaskKey, variables);
    final CompletableFuture<ResponseEntity<Void>> finalResponse = new CompletableFuture<>();
    final CompletableFuture<BrokerResponse<UserTaskRecord>> responseCompletableFuture =
        brokerClient.sendRequestWithRetry(request);
    responseCompletableFuture.whenComplete(
        (userTaskRecordBrokerResponse, throwable) -> {
          if (throwable != null) {
            finalResponse.complete(new ResponseEntity<>(HttpStatus.BAD_REQUEST));
            return;
          }

          finalResponse.complete(new ResponseEntity<>(HttpStatus.NO_CONTENT));
        });
    return finalResponse;
  }
}
