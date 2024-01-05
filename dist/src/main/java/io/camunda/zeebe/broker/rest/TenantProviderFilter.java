/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.rest;

import java.util.List;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

@Component
public class TenantProviderFilter implements WebFilter {

  public static final String TENANT_CTX_KEY = "io.camunda.zeebe.broker.rest.tenandIds";
  private static final List<String> TENANTS = List.of("foo", "bar");

  @Override
  public Mono<Void> filter(final ServerWebExchange exchange, final WebFilterChain chain) {
    exchange.getAttributes().put(TENANT_CTX_KEY, TENANTS);
    return chain.filter(exchange);
  }
}
