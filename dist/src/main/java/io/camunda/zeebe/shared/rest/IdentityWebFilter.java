/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.shared.rest;

import io.camunda.identity.sdk.Identity;
import io.camunda.zeebe.shared.DisableRestApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

@Component
@ConditionalOnMissingBean(DisableRestApi.class)
public class IdentityWebFilter implements WebFilter {
  private static final Logger LOGGER = LoggerFactory.getLogger(IdentityWebFilter.class);

  private final Identity identity;

  @Autowired
  public IdentityWebFilter(final Identity identity) {
    this.identity = identity;
  }

  @Override
  public Mono<Void> filter(final ServerWebExchange exchange, final WebFilterChain chain) {
    //    final var request = exchange.getRequest();
    //    final var authorization = request.getHeaders().get(HttpHeaders.AUTHORIZATION);
    //    if (authorization == null) {
    //      LOGGER.debug("Denying call {} as no token was provided", request.getPath());
    //      exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
    //      return Mono.empty();
    //    }
    //
    //    final String token = authorization.get(0).replaceFirst("^Bearer ", "");
    //    try {
    //      identity.authentication().verifyToken(token);
    //    } catch (final TokenVerificationException e) {
    //      LOGGER.debug(
    //          "Denying call {} as the token could not be fully verified. Error message: {}",
    //          request.getPath(),
    //          e.getMessage());
    //
    //      exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
    //      return Mono.empty();
    //    }

    return chain.filter(exchange);
  }
}
