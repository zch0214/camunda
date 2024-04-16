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

public record BrokerConfig(Map<Integer, PartitionConfig> partitionConfigs) {
  // TODO: allow capability to remove brokers and partitions. Right now when merging, removed
  // brokers can be re-added.
  BrokerConfig merge(final BrokerConfig other) {
    final var mergedPartitions = new HashMap<>(partitionConfigs);
    other
        .partitionConfigs()
        .forEach(
            (key, value) ->
                mergedPartitions.put(key, mergedPartitions.getOrDefault(key, value).merge(value)));
    return new BrokerConfig(Map.copyOf(mergedPartitions));
  }
}
