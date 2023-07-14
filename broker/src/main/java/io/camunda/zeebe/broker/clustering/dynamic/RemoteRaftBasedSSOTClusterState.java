/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic;

import io.atomix.raft.partition.RaftPartition;
import java.util.concurrent.CompletableFuture;

// Running on all other nodes
public class RemoteRaftBasedSSOTClusterState implements SSOTClusterState {
  RaftPartition partition;
  Cluster cluster;

  @Override
  public CompletableFuture<Cluster> getClusterState() {
    // Find leader of system partition
    // Send remote request to that partition
    return new CompletableFuture<>();
  }
}
