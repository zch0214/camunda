/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.qa.util.actuator;

import feign.Feign;
import feign.Headers;
import feign.Param;
import feign.RequestLine;
import feign.Retryer;
import feign.Target.HardCodedTarget;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import io.camunda.zeebe.qa.util.cluster.TestApplication;
import io.zeebe.containers.ZeebeNode;

public interface ExporterActuator {
  static ExporterActuator of(final ZeebeNode<?> node) {
    final var endpoint =
        String.format("http://%s/actuator/exporters", node.getExternalMonitoringAddress());
    return of(endpoint);
  }

  static ExporterActuator of(final TestApplication<?> node) {
    return of(node.actuatorUri("exporters").toString());
  }

  static ExporterActuator of(final String endpoint) {
    final var target = new HardCodedTarget<>(ExporterActuator.class, endpoint);
    return Feign.builder()
        .encoder(new JacksonEncoder())
        .decoder(new JacksonDecoder())
        .retryer(Retryer.NEVER_RETRY)
        .target(target);
  }

  /**
   * @throws feign.FeignException if the request is not successful (e.g. 4xx or 5xx)
   */
  @RequestLine("POST /enable/{id}")
  @Headers({"Content-Type: application/json", "Accept: application/json"})
  void enable(@Param String id);

  /**
   * @throws feign.FeignException if the request is not successful (e.g. 4xx or 5xx)
   */
  @RequestLine("POST /disable/{id}")
  @Headers({"Content-Type: application/json", "Accept: application/json"})
  void disable(@Param String id);
}
