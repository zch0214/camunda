/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic;

import io.atomix.cluster.MemberId;
import java.util.concurrent.CompletableFuture;

public interface ConfigCoordinator {

  CompletableFuture<Void> start();

  CompletableFuture<Void> addMember(final MemberId memberId);

  CompletableFuture<Void> leaveMember();

  /**
   * Can be called during broker startup, for example by PartitionManagerStep to find the partition
   * distribution. Startup should be blocked until cluster configuration is available.
   */
  CompletableFuture<Cluster> getCluster();
}
