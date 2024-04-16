/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.topology.configurations;

import java.util.Optional;

/**
 * Assumptions: exporter state of a partition in a broker is updated only by the broker itself or by
 * the coordinator. This simplifies the possible concurrent operations. If we want to allow
 * concurrent updates by any broker, we have to define a proper CRDT version of this.
 */
public record ExporterState(long version, State state, Optional<String> initializeFrom) {

  ExporterState disabling() {
    if (state == State.ENABLED) {
      return new ExporterState(version + 1, State.DISABLING, initializeFrom);
    } else if (state == State.ENABLING) {
      // this is a concurrent disabling, while the broker has not completed the previous enable
      // request
      return new ExporterState(version + 2, State.DISABLING, initializeFrom);
    } else {
      return this;
    }
  }

  ExporterState disabled() {
    if (state == State.DISABLING) {
      return new ExporterState(version + 1, State.DISABLED, initializeFrom);
    } else {
      return this;
    }
  }

  ExporterState enabling() {
    return enabling(null);
  }

  ExporterState enabling(final String intializeFrom) {
    if (state == State.DISABLED) {
      return new ExporterState(version + 1, State.ENABLING, Optional.ofNullable(intializeFrom));
    } else if (state == State.DISABLING) {
      return new ExporterState(version + 2, State.ENABLING, Optional.ofNullable(intializeFrom));
    } else {
      return this;
    }
  }

  ExporterState enabled() {
    if (state == State.ENABLING) {
      return new ExporterState(version + 1, State.ENABLED, initializeFrom);
    } else {
      return this;
    }
  }

  ExporterState merge(final ExporterState other) {
    if (version >= other.version) {
      return this;
    } else {
      return other;
    }
  }

  private enum State {
    ENABLED,
    DISABLING,
    DISABLED,
    ENABLING
  }
}
