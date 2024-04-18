/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.topology.state;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public record DynamicConfiguration(Map<String, ExporterConfig> exporters) {

  public DynamicConfiguration disableExporter(final String exporterId) {
    final HashMap<String, ExporterConfig> map = new HashMap<>(exporters);
    map.computeIfPresent(
        exporterId,
        (id, config) ->
            new ExporterConfig(ExporterConfig.ExporterState.DISABLED, Optional.empty()));
    return new DynamicConfiguration(Map.copyOf(map));
  }

  public DynamicConfiguration enableExporter(final String exporterId) {
    // TODO: allow specifying initialize from
    final HashMap<String, ExporterConfig> map = new HashMap<>(exporters);
    map.computeIfPresent(
        exporterId,
        (id, config) -> new ExporterConfig(ExporterConfig.ExporterState.ENABLED, Optional.empty()));
    return new DynamicConfiguration(Map.copyOf(map));
  }

  public record ExporterConfig(
      ExporterState state, Optional<ExporterInitializationConfig> initializeFrom) {

    public record ExporterInitializationConfig(String initializeFrom, long metadataVersion) {}

    public enum ExporterState {
      ENABLED,
      DISABLED,
    }
  }
}
