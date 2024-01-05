/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.gateway.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.HttpResult;
import io.camunda.zeebe.auth.api.JwtAuthorizationBuilder;
import io.camunda.zeebe.auth.impl.Authorization;
import io.camunda.zeebe.gateway.impl.broker.BrokerClient;
import io.camunda.zeebe.gateway.impl.broker.request.BrokerUserTaskCompleteRequest;
import io.camunda.zeebe.gateway.impl.configuration.MultiTenancyCfg;
import io.camunda.zeebe.protocol.record.value.TenantOwned;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public final class UserTaskApiImpl implements UserTaskApi {
  private static final ObjectWriter SERIALIZER =
      new ObjectMapper().writerFor(new TypeReference<Map<String, Object>>() {});
  private final BrokerClient brokerClient;
  private final MultiTenancyCfg multiTenancy;

  public UserTaskApiImpl(final BrokerClient brokerClient, final MultiTenancyCfg multiTenancy) {
    this.brokerClient = brokerClient;
    this.multiTenancy = multiTenancy;
  }

  @Override
  public CompletableFuture<HttpResult<Void>> complete(
      final ServiceRequestContext context, final long key, final Map<String, Object> variables) {
    context.setShouldReportUnhandledExceptions(false);

    final String serialized;
    try {
      serialized = SERIALIZER.writeValueAsString(variables);
    } catch (final JsonProcessingException e) {
      return CompletableFuture.completedFuture(HttpResult.of(HttpStatus.BAD_REQUEST));
    }

    return sendRequest(key, serialized, context.attr(TenantProviderDecorator.TENANT_CTX_KEY));
  }

  public CompletableFuture<HttpResult<Void>> sendRequest(
      final Long userTaskKey, final String variables, final List<String> authorizedTenants) {
    final var request = RestRequestMapper.toCompleteUserTaskRequest(userTaskKey, variables);

    applyMultiTenancy(
        multiTenancy.isEnabled()
            ? authorizedTenants
            : List.of(TenantOwned.DEFAULT_TENANT_IDENTIFIER),
        request);

    return brokerClient
        .sendRequestWithRetry(request)
        .handle(
            (ok, error) -> {
              if (error != null) {
                return HttpResult.of(HttpStatus.BAD_REQUEST);
              }

              return HttpResult.of(HttpStatus.NO_CONTENT);
            });
  }

  private void applyMultiTenancy(
      final List<String> authorizedTenants, final BrokerUserTaskCompleteRequest request) {
    final String authorizationToken =
        Authorization.jwtEncoder()
            .withIssuer(JwtAuthorizationBuilder.DEFAULT_ISSUER)
            .withAudience(JwtAuthorizationBuilder.DEFAULT_AUDIENCE)
            .withSubject(JwtAuthorizationBuilder.DEFAULT_SUBJECT)
            .withClaim(Authorization.AUTHORIZED_TENANTS, authorizedTenants)
            .encode();
    request.setAuthorization(authorizationToken);
  }
}
