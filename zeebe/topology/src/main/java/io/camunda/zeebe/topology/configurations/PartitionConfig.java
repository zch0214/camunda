/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.topology.configurations;

import java.util.HashMap;
import java.util.Map;

public record PartitionConfig(Map<String, ExporterStateCRDT> exporters) {

  PartitionConfig merge(final PartitionConfig other) {
    final var mergedExporters = new HashMap<>(exporters);
    other
        .exporters()
        .forEach(
            (key, value) ->
                mergedExporters.put(key, mergedExporters.getOrDefault(key, value).merge(value)));
    return new PartitionConfig(Map.copyOf(mergedExporters));
  }
}
