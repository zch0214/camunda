/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.partitioning;

import java.util.Map;
import java.util.Objects;

public final class DynamicPartitionConfig {
  private final DynamicExportersConfig config;

  public DynamicPartitionConfig(final DynamicExportersConfig config) {
    this.config = config;
  }

  public DynamicExportersConfig config() {
    return config;
  }

  @Override
  public int hashCode() {
    return Objects.hash(config);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != getClass()) {
      return false;
    }
    final var that = (DynamicPartitionConfig) obj;
    return Objects.equals(config, that.config);
  }

  @Override
  public String toString() {
    return "DynamicPartitionConfig[" + "config=" + config + ']';
  }

  static final class DynamicExporterConfig {
    private State state;

    DynamicExporterConfig(final State state) {
      this.state = state;
    }

    public void setState(final State state) {
      this.state = state;
    }

    public State state() {
      return state;
    }

    @Override
    public int hashCode() {
      return Objects.hash(state);
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj == null || obj.getClass() != getClass()) {
        return false;
      }
      final var that = (DynamicExporterConfig) obj;
      return Objects.equals(state, that.state);
    }

    @Override
    public String toString() {
      return "DynamicExporterConfig[" + "state=" + state + ']';
    }

    enum State {
      ENABLED,
      DISABLED,
    }
  }

  static final class DynamicExportersConfig {
    private final Map<String, DynamicExporterConfig> exporters;

    DynamicExportersConfig(final Map<String, DynamicExporterConfig> exporters) {
      this.exporters = exporters;
    }

    public Map<String, DynamicExporterConfig> exporters() {
      return exporters;
    }

    @Override
    public int hashCode() {
      return Objects.hash(exporters);
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj == null || obj.getClass() != getClass()) {
        return false;
      }
      final var that = (DynamicExportersConfig) obj;
      return Objects.equals(exporters, that.exporters);
    }

    @Override
    public String toString() {
      return "DynamicExportersConfig[" + "exporters=" + exporters + ']';
    }
  }
}
