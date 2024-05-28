/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.shared.management;

import com.netflix.concurrency.limits.Limit;
import io.camunda.zeebe.broker.system.configuration.FlowControlCfg;
import io.camunda.zeebe.logstreams.impl.flowcontrol.LimitType;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;
import org.springframework.boot.actuate.endpoint.web.annotation.RestControllerEndpoint;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@Component
@RestControllerEndpoint(id = "flowControl")
public class FlowControlEndpoint {

  final FlowControlService flowControlService;

  @Autowired
  public FlowControlEndpoint(final FlowControlService flowControlService) {
    this.flowControlService = flowControlService;
  }

  @PostMapping()
  public WebEndpointResponse<?> post(@RequestBody final FlowControlCfg flowControlCfg) {

    try {
      flowControlService.set(flowControlCfg);
      return new WebEndpointResponse<>(WebEndpointResponse.STATUS_NO_CONTENT);
    } catch (final Exception e) {
      return new WebEndpointResponse<>(e, WebEndpointResponse.STATUS_INTERNAL_SERVER_ERROR);
    }
  }

  @GetMapping
  public ResponseEntity<?> get() {
    try {
      return ResponseEntity.status(WebEndpointResponse.STATUS_OK)
          .body(flowControlService.get().join());
    } catch (final Exception e) {
      return ResponseEntity.internalServerError().body(e);
    }
  }

  interface FlowControlService {
    CompletableFuture<Map<Integer, Map<LimitType, Limit>>> get();

    CompletableFuture<Void> set(FlowControlCfg flowControlCfg);
  }
}
