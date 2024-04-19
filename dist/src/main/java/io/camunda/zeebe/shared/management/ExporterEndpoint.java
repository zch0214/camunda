/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.shared.management;

import io.camunda.zeebe.topology.api.ErrorResponse;
import io.camunda.zeebe.topology.api.TopologyChangeResponse;
import io.camunda.zeebe.topology.api.TopologyManagementRequest.DisableExporterRequest;
import io.camunda.zeebe.topology.api.TopologyManagementRequest.EnableExporterRequest;
import io.camunda.zeebe.topology.api.TopologyManagementRequestSender;
import io.camunda.zeebe.topology.state.ClusterTopology;
import io.camunda.zeebe.util.Either;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.endpoint.web.annotation.RestControllerEndpoint;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;

@Component
@RestControllerEndpoint(id = "exporters")
public class ExporterEndpoint {
  private final TopologyManagementRequestSender requestSender;

  @Autowired
  public ExporterEndpoint(final TopologyManagementRequestSender requestSender) {
    this.requestSender = requestSender;
  }

  @GetMapping(produces = "application/json")
  public ResponseEntity<?> clusterTopology() {
    try {
      final var response = requestSender.getTopology().join();
      return mapResponse(response);
    } catch (final Exception error) {
      return ResponseEntity.status(500).body(error.getMessage());
    }
  }

  private static ResponseEntity<? extends Record> mapResponse(
      final Either<ErrorResponse, ClusterTopology> response) {
    if (response.isRight()) {
      return ResponseEntity.status(200).body(response.get());
    } else {
      return ResponseEntity.status(500).body(response.getLeft());
    }
  }

  @PostMapping(path = "/{operation}/{id}")
  public ResponseEntity<?> add(
      @PathVariable("operation") final Operation resource, @PathVariable final String id) {
    try {
      return switch (resource) {
        case enable ->
            mapChangeResponse(
                requestSender.enableExporter(new EnableExporterRequest(id, false)).join());
        case disable ->
            mapChangeResponse(
                requestSender.disableExporter(new DisableExporterRequest(id, false)).join());
      };
    } catch (final Exception error) {
      LoggerFactory.getLogger("FINDME").error("Error:", error);
      return ResponseEntity.status(500).body(error.getMessage());
    }
  }

  private ResponseEntity<?> mapChangeResponse(
      final Either<ErrorResponse, TopologyChangeResponse> response) {
    if (response.isRight()) {
      return ResponseEntity.status(200).body("ok");
    } else {
      return ResponseEntity.status(500).body(response.getLeft());
    }
  }

  private enum Operation {
    enable,
    disable
  }
}
