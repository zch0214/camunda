/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.topology.configurations;

import java.util.Map;

public record GlobalConfig(long hash, Map<Integer, BrokerConfig> brokerConfigs) {

  public GlobalConfig(final Map<Integer, BrokerConfig> brokerConfigs) {
    // May be replace with a better hash code with fewer collisions
    this(brokerConfigs.hashCode(), brokerConfigs);
  }

  GlobalConfig merge(final GlobalConfig other) {
    if (hash == other.hash) {
      // optimization for unnecessary merges if the state is the same
      return this;
    }

    final var mergedBrokers = Map.copyOf(brokerConfigs);
    other
        .brokerConfigs()
        .forEach(
            (key, value) ->
                mergedBrokers.put(key, mergedBrokers.getOrDefault(key, value).merge(value)));
    return new GlobalConfig(Map.copyOf(mergedBrokers));
  }
}
