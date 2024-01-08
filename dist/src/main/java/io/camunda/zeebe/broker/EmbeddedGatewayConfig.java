/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker;

import io.camunda.zeebe.shared.EmbeddedGatewayService;
import java.nio.charset.StandardCharsets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.WebFilter;
import reactor.core.publisher.Mono;

@Configuration
public class EmbeddedGatewayConfig {
  private final Properties properties;

  @Autowired
  public EmbeddedGatewayConfig(final Properties properties) {
    this.properties = properties;
  }

  @ConditionalOnProperty(name = "zeebe.broker.gateway.enable", havingValue = "true")
  @Bean
  public EmbeddedGatewayService embeddedGatewayService() {
    return new EmbeddedGatewayService();
  }

  @ConditionalOnProperty(name = "zeebe.broker.gateway.enable", havingValue = "false")
  @Bean
  public WebFilter embeddedGatewayDisabled() {
    return (exchange, chain) -> {
      if (exchange.getRequest().getLocalAddress().getPort() == properties.port) {
        final var bytes = "Embedded gateway is disabled".getBytes(StandardCharsets.UTF_8);
        final var buffer = exchange.getResponse().bufferFactory().wrap(bytes);
        exchange.getResponse().setStatusCode(HttpStatus.NOT_IMPLEMENTED);
        return exchange.getResponse().writeWith(Mono.just(buffer));
      }

      return chain.filter(exchange);
    };
  }

  @ConfigurationProperties(prefix = "server")
  record Properties(int port) {}
}
