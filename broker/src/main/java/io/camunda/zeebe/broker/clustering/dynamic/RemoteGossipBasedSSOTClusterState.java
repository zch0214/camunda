/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic;

import java.util.concurrent.CompletableFuture;

public class RemoteGossipBasedSSOTClusterState implements SSOTClusterState {

  @Override
  public CompletableFuture<Cluster> getClusterState() {
    // Find coordinator
    // Send remote request
    return new CompletableFuture<>();
  }
}
