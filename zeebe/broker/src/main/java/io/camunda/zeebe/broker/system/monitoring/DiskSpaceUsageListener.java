/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.broker.system.monitoring;

/**
 * Used by DiskSpaceUsageMonitor to notify listeners when disk space usage grows above (and below)
 * the configured threshold
 */
public interface DiskSpaceUsageListener {

  /** Will be called when disk space usage grows above the threshold */
  default void onDiskSpaceNotAvailable() {}

  /** Will be called when disk space usage goes below the threshold after it was above it. */
  default void onDiskSpaceAvailable() {}
}
