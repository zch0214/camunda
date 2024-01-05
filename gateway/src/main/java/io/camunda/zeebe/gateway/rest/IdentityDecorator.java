/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.gateway.rest;

import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.DecoratingHttpServiceFunction;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.camunda.identity.sdk.Identity;
import io.camunda.identity.sdk.IdentityConfiguration;
import io.camunda.identity.sdk.authentication.exception.TokenVerificationException;
import io.camunda.identity.sdk.tenants.dto.Tenant;
import io.camunda.zeebe.gateway.impl.configuration.MultiTenancyCfg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdentityDecorator implements DecoratingHttpServiceFunction {
  private static final Logger LOGGER = LoggerFactory.getLogger(IdentityDecorator.class);

  private final Identity identity;
  private final MultiTenancyCfg multiTenancy;

  public IdentityDecorator(
      final IdentityConfiguration configuration, final MultiTenancyCfg multiTenancy) {
    this(new Identity(configuration), multiTenancy);
  }

  public IdentityDecorator(final Identity identity, final MultiTenancyCfg multiTenancy) {
    this.identity = identity;
    this.multiTenancy = multiTenancy;
  }

  @Override
  public HttpResponse serve(
      final HttpService delegate, final ServiceRequestContext ctx, final HttpRequest req)
      throws Exception {
    final var operation = req.path();

    final var authorization = req.headers().get(HttpHeaderNames.AUTHORIZATION);
    if (authorization == null) {
      LOGGER.debug("Denying call {} as no token was provided", operation);
      return HttpResponse.of(
          HttpStatus.UNAUTHORIZED,
          MediaType.PLAIN_TEXT_UTF_8,
          "Expected bearer token at header with key [%s], but found nothing"
              .formatted(HttpHeaderNames.AUTHORIZATION));
    }

    final String token = authorization.replaceFirst("^Bearer ", "");
    try {
      identity.authentication().verifyToken(token);
    } catch (final TokenVerificationException e) {
      LOGGER.debug(
          "Denying call {} as the token could not be fully verified. Error message: {}",
          operation,
          e.getMessage());

      return HttpResponse.of(
          HttpStatus.UNAUTHORIZED,
          MediaType.PLAIN_TEXT_UTF_8,
          "Failed to parse bearer token, see cause for details");
    }

    if (!multiTenancy.isEnabled()) {
      return delegate.serve(ctx, req);
    }

    try {
      final var authorizedTenants =
          identity.tenants().forToken(token).stream().map(Tenant::getTenantId).toList();
      ctx.setAttr(TenantProviderDecorator.TENANT_CTX_KEY, authorizedTenants);
      return delegate.serve(ctx, req);
    } catch (final RuntimeException e) {
      LOGGER.debug(
          "Denying call {} as the authorized tenants could not be retrieved from Identity. Error message: {}",
          operation,
          e.getMessage());
      return HttpResponse.of(
          HttpStatus.UNAUTHORIZED,
          MediaType.PLAIN_TEXT_UTF_8,
          "Expected Identity to provide authorized tenants, see cause for details");
    }
  }
}
