/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.topology.configurations;

import io.camunda.zeebe.scheduler.future.ActorFuture;
import io.camunda.zeebe.scheduler.future.CompletableActorFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationManager.class);

  private final int localBrokerId;
  private GlobalConfig localConfig = null;

  public ConfigurationManager(final int localBrokerId) {
    this.localBrokerId = localBrokerId;
  }

  public void onConfigurationReceivedViaGossip(final GlobalConfig receivedConfig) {
    if (localConfig == null) {
      localConfig = receivedConfig;
    } else {
      final var mergedConfig = localConfig.merge(receivedConfig);
      // this is just an optimization for equality check, may be replaced with something better
      if (mergedConfig.hash() == localConfig.hash()) {
        // nothing to do
        return;
      }
      final var prevConfig = localConfig;
      localConfig = mergedConfig;
      final var operationsToApply = getNewOperationsToApply(prevConfig, mergedConfig);
      LOGGER.info("Applying configuration changes {}", operationsToApply);
      operationsToApply.forEach(this::applyOperation);
    }
  }

  private void applyOperation(final ConfigChangeOperation o) {
    final ConfigChangeOperationApplier applier = getApplier(o);
    applier
        .apply()
        .onComplete(
            (f, error) -> {
              // TODO: handle error
              final var newBrokerConfig = f.apply(localConfig.brokerConfigs().get(localBrokerId));
              // update global config with newBrokerConfig
            });
  }

  private ConfigChangeOperationApplier getApplier(final ConfigChangeOperation o) {
    return new ConfigChangeOperationApplier() {
      @Override
      public ActorFuture<Function<BrokerConfig, BrokerConfig>> apply() {
        // TODO: apply change, eg:- exporter enabled -> disabled
        return CompletableActorFuture.completed(Function.identity());
      }
    };
  }

  private List<ConfigChangeOperation> getNewOperationsToApply(
      final GlobalConfig prevConfig, final GlobalConfig mergedConfig) {
    final ArrayList<ConfigChangeOperation> operationsToApply = new ArrayList<>();
    prevConfig
        .brokerConfigs()
        .get(localBrokerId)
        .partitionConfigs()
        .forEach(
            (partitionId, prevPartitionConfig) -> {
              final var newPartitionConfig =
                  mergedConfig
                      .brokerConfigs()
                      .get(localBrokerId)
                      .partitionConfigs()
                      .get(partitionId);
              if (prevPartitionConfig.equals(newPartitionConfig)) {
                // nothing to do
                return;
              } else {
                final var operationsOnPartition =
                    getNewOperations(partitionId, newPartitionConfig, prevPartitionConfig);
                operationsToApply.addAll(operationsOnPartition);
              }
            });
    return operationsToApply;
  }

  private List<ConfigChangeOperation> getNewOperations(
      final Integer partitionId,
      final PartitionConfig newPartitionConfig,
      final PartitionConfig prevPartitionConfig) {
    return newPartitionConfig.exporters().entrySet().stream()
        .map(
            entry -> {
              final var exporterId = entry.getKey();
              final var newExporterState = entry.getValue();
              final var prevExporterState = prevPartitionConfig.exporters().get(exporterId);
              if (newExporterState.equals(prevExporterState)) {
                return null;
              } else if (newExporterState.operations().isEmpty()) {
                return null;
              } else {
                final var operation = newExporterState.operations().getFirst();
                if (!prevExporterState.operations().isEmpty()
                    && operation.equals(prevExporterState.operations().getFirst())) {
                  return null;
                }
                return new ConfigChangeOperation(
                    partitionId, new ExporterConfigOperation(operation));
              }
            })
        .toList();
  }

  record ConfigChangeOperation(int partitionId, ConfigOperation operation) {}

  record ExporterConfigOperation(ExporterStateCRDT.Operation operation)
      implements ConfigOperation {}

  interface ConfigOperation {}
}
