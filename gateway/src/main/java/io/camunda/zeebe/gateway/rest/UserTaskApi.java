/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.gateway.rest;

import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.Consumes;
import com.linecorp.armeria.server.annotation.HttpResult;
import com.linecorp.armeria.server.annotation.JacksonRequestConverterFunction;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.RequestConverter;
import com.linecorp.armeria.server.annotation.ServiceName;
import com.linecorp.armeria.server.annotation.StatusCode;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@ServiceName("userTasks")
public interface UserTaskApi {
  @StatusCode(204)
  @Consumes("application/json")
  @Post("/api/v1/userTasks/{key}/complete")
  CompletableFuture<HttpResult<Void>> complete(
      final ServiceRequestContext context,
      @Param("key") final long key,
      @RequestConverter(JacksonRequestConverterFunction.class) final Map<String, Object> variables);
}
