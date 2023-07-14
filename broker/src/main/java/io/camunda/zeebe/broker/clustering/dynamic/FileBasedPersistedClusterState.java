/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic;

public class FileBasedPersistedClusterState implements LocalPersistedClusterState {
  Cluster cluster;

  void initialize() {
    // TODO
    // read from local file
  }

  @Override
  public Cluster getClusterState() {
    return cluster;
  }

  @Override
  public void setClusterState(final Cluster cluster) {
    this.cluster = cluster;
    // Write to file
  }
}
