/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.topology.state;

import java.util.Map;
import java.util.function.UnaryOperator;

public record PartitionState(State state, int priority, DynamicConfiguration config) {
  public static PartitionState active(final int priority) {
    return new PartitionState(State.ACTIVE, priority, new DynamicConfiguration(Map.of()));
  }

  public static PartitionState joining(final int priority) {
    return new PartitionState(State.JOINING, priority, new DynamicConfiguration(Map.of()));
  }

  public PartitionState toActive() {
    if (state == State.LEAVING) {
      throw new IllegalStateException(
          String.format("Cannot transition to ACTIVE when current state is %s", state));
    }
    return new PartitionState(State.ACTIVE, priority, config);
  }

  public PartitionState toLeaving() {
    return new PartitionState(State.LEAVING, priority, config);
  }

  public PartitionState updateConfig(final UnaryOperator<DynamicConfiguration> configUpdater) {
    return new PartitionState(state, priority, configUpdater.apply(config));
  }

  public enum State {
    UNKNOWN,
    JOINING,
    ACTIVE,
    LEAVING
  }
}
