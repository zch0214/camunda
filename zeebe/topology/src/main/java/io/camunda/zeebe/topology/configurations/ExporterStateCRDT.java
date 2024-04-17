/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.topology.configurations;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

public record ExporterStateCRDT(List<Operation> operations, StateCRDT state) {
  ExporterStateCRDT merge(final ExporterStateCRDT other) {
    final var mergedState = state.merge(other.state());

    final var mergedOperations = new HashSet<>(operations);
    mergedOperations.addAll(other.operations());
    final var mergedList =
        mergedOperations.stream()
            // Take only non-applied operations. This is a form of garbage collection
            .filter(o -> o.uniqueId > mergedState.idOfLastOperation())
            .sorted(Comparator.comparingLong(Operation::uniqueId))
            .toList();

    return new ExporterStateCRDT(mergedList, mergedState);
  }

  record Operation(String operation, long uniqueId) {
    // uniqueId could be anything that is unique and comparable like SnowflakeId
    // Concurrent operations will be ordered by the uniqueId as a way of conflict resolution. That
    // means we don't need a dedicated broker to act as the coordinator.
  }

  private record StateCRDT(State state, long idOfLastOperation) {

    StateCRDT merge(final StateCRDT other) {
      if (idOfLastOperation >= other.idOfLastOperation) {
        return this;
      } else {
        return other;
      }
    }
  }

  enum State {
    ENABLED,
    DISABLED,
  }
}
